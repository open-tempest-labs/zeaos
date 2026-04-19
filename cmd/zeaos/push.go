package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/open-tempest-labs/zeaberg-go"
)

// ---------------------------------------------------------------------------
// Push records — persisted in TableEntry and zea_export.json
// ---------------------------------------------------------------------------

// PushRecord captures a single push event for a session table.
type PushRecord struct {
	Target    string    `json:"target"`               // e.g. "md:my_database" or "zea://s3-data/exports"
	Schema    string    `json:"schema"`               // schema/dataset written into
	TableName string    `json:"table_name"`           // name used in the target
	PushedAt  time.Time `json:"pushed_at"`
	RowCount  int64     `json:"row_count"`
	SourceURI string    `json:"source_uri,omitempty"` // original HTTPS/S3 URI
	S3URI     string    `json:"s3_uri,omitempty"`     // canonical S3 URI (ZeaDrive targets only)
	Format    string    `json:"format,omitempty"`     // "iceberg" or "" (flat parquet)
}

// ---------------------------------------------------------------------------
// Push arguments
// ---------------------------------------------------------------------------

type pushArgs struct {
	Subcommand string   // "", "status", "sync"
	Target     string   // "md:database", "s3://...", etc.
	Tables     []string // specific session tables; empty = all promoted sources
	Schema     string   // override target schema name (default: "zea_exports")
	DryRun     bool
	Iceberg    bool // write Iceberg table format (ZeaDrive targets only)
}

func parsePushArgs(args []string) (*pushArgs, error) {
	pa := &pushArgs{}

	if len(args) == 0 {
		return nil, fmt.Errorf("push: target required — e.g. push --target md:my_database\n" +
			"  push status                     show push history\n" +
			"  push sync --target md:database  check for drift and re-push if stale")
	}

	// Subcommands: status, sync
	switch args[0] {
	case "status":
		pa.Subcommand = "status"
		return pa, nil
	case "sync":
		pa.Subcommand = "sync"
		args = args[1:]
	}

	for i := 0; i < len(args); i++ {
		switch {
		case strings.HasPrefix(args[i], "--target="):
			pa.Target = strings.TrimPrefix(args[i], "--target=")
		case args[i] == "--target" && i+1 < len(args):
			i++
			pa.Target = args[i]
		case strings.HasPrefix(args[i], "--schema="):
			pa.Schema = strings.TrimPrefix(args[i], "--schema=")
		case args[i] == "--schema" && i+1 < len(args):
			i++
			pa.Schema = args[i]
		case args[i] == "--dry-run":
			pa.DryRun = true
		case args[i] == "--iceberg":
			pa.Iceberg = true
		case !strings.HasPrefix(args[i], "--"):
			pa.Tables = append(pa.Tables, args[i])
		}
	}

	if pa.Target == "" && pa.Subcommand != "status" {
		cfg, _ := loadConfig()
		if cfg != nil {
			if cfg.Push.DefaultTarget != "" {
				pa.Target = cfg.Push.DefaultTarget
			}
			if pa.Schema == "" && cfg.Push.DefaultSchema != "" {
				pa.Schema = cfg.Push.DefaultSchema
			}
		}
	}

	if pa.Target == "" && pa.Subcommand != "status" {
		return nil, fmt.Errorf("push: --target required (or set a default with push --target md:db --set-default)")
	}

	return pa, nil
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

func execPush(args []string, s *Session) error {
	pa, err := parsePushArgs(args)
	if err != nil {
		return err
	}

	switch pa.Subcommand {
	case "status":
		return execPushStatus(s)
	case "sync":
		return execPushSync(pa, s)
	default:
		return execPushData(pa, s)
	}
}

// ---------------------------------------------------------------------------
// push <tables> --target md:database
// ---------------------------------------------------------------------------

func execPushData(pa *pushArgs, s *Session) error {
	tables, err := resolvePushTables(pa, s)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("push: no tables to push — promote tables first or specify names explicitly")
	}

	// Prompt for warehouse path extension and/or schema if not yet configured.
	if pa.Schema == "" {
		if err := promptWarehouseSchema(pa); err != nil {
			return err
		}
	}

	target, err := openPushTarget(pa, s)
	if err != nil {
		return err
	}
	defer target.close()

	fmt.Printf("Pushing %d table(s) to %s...\n", len(tables), pa.Target)

	for _, entry := range tables {
		rec, err := target.push(entry, pa.Schema, pa.DryRun, s)
		if err != nil {
			fmt.Printf("  ✗ %s: %v\n", entry.Name, err)
			continue
		}
		if pa.DryRun {
			fmt.Printf("  (dry-run) %s → %s.%s  %d rows\n",
				entry.Name, rec.Schema, rec.TableName, rec.RowCount)
			continue
		}
		// Record the push in the table entry.
		entry.PushRecords = append(entry.PushRecords, *rec)
		fmt.Printf("  ✓ %s → %s.%s  %d rows\n",
			entry.Name, rec.Schema, rec.TableName, rec.RowCount)
	}

	if !pa.DryRun {
		_ = s.saveRegistry()
		// Persist target and schema so subsequent pushes don't re-prompt.
		cfg, _ := loadConfig()
		if cfg == nil {
			cfg = &zeaosConfig{}
		}
		changed := false
		if cfg.Push.DefaultTarget != pa.Target {
			cfg.Push.DefaultTarget = pa.Target
			changed = true
		}
		if pa.Schema != "" && cfg.Push.DefaultSchema != pa.Schema {
			cfg.Push.DefaultSchema = pa.Schema
			changed = true
		}
		if changed {
			_ = saveConfig(cfg)
		}
		fmt.Printf("Push complete. Run 'push status' to review.\n")

		// Update the staging macro in the published dbt repo when pushing to
		// MotherDuck — adds a guard so the macro skips HTTPS re-fetch in prod.
		// Not applicable for ZeaDrive or other targets.
		if strings.HasPrefix(pa.Target, "md:") {
			var sources []sourceEntry
			for _, entry := range tables {
				if strings.HasPrefix(entry.SourceURI, "http://") || strings.HasPrefix(entry.SourceURI, "https://") {
					sn, tbl, desc := mapURIToSource(entry.SourceURI)
					sources = append(sources, sourceEntry{SourceName: sn, TableName: tbl, Description: desc, URI: entry.SourceURI})
				}
			}
			if len(sources) > 0 {
				fmt.Printf("Updating staging macro in published repo...\n")
				if err := updateRepoStagingMacro(pa.Target, sources, s); err != nil {
					fmt.Printf("  ⚠  could not update repo macro: %v\n", err)
				}
			}
		}
	}
	return nil
}

// resolvePushTables returns the session tables to push.
// Explicit table names on the command line take priority; otherwise all
// non-internal session tables are pushed. To push only the source data for
// promoted models, use 'model push' instead.
func resolvePushTables(pa *pushArgs, s *Session) ([]*TableEntry, error) {
	if len(pa.Tables) > 0 {
		var out []*TableEntry
		for _, name := range pa.Tables {
			e, err := s.Get(name)
			if err != nil {
				return nil, err
			}
			out = append(out, e)
		}
		return out, nil
	}

	var out []*TableEntry
	for _, entry := range s.Registry {
		if !strings.HasPrefix(entry.Name, "_") {
			out = append(out, entry)
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// push status
// ---------------------------------------------------------------------------

func execPushStatus(s *Session) error {
	type row struct {
		table, target, pushedAt, format, rows string
	}
	var rows []row
	for _, entry := range s.Registry {
		for _, rec := range entry.PushRecords {
			format := rec.Format
			if format == "" {
				format = "parquet"
			}
			rows = append(rows, row{
				table:    entry.Name,
				target:   rec.Target + "/" + rec.Schema + "/" + rec.TableName,
				pushedAt: rec.PushedAt.Format("2006-01-02 15:04:05"),
				format:   format,
				rows:     fmt.Sprintf("%d", rec.RowCount),
			})
		}
	}
	if len(rows) == 0 {
		fmt.Println("No push history. Use 'push --target md:database' to push session tables.")
		return nil
	}

	// Compute column widths from data.
	w := [4]int{5, 6, 19, 6} // minimums: Table, Target, Pushed At, Format
	for _, r := range rows {
		if len(r.table) > w[0] {
			w[0] = len(r.table)
		}
		if len(r.target) > w[1] {
			w[1] = len(r.target)
		}
		if len(r.format) > w[3] {
			w[3] = len(r.format)
		}
	}

	sep := strings.Repeat("─", w[0]+w[1]+w[2]+w[3]+14)
	fmt.Printf("%-*s  %-*s  %-*s  %-*s  %s\n", w[0], "Table", w[1], "Target", w[2], "Pushed At", w[3], "Format", "Rows")
	fmt.Println(sep)
	for _, r := range rows {
		fmt.Printf("%-*s  %-*s  %-*s  %-*s  %s\n", w[0], r.table, w[1], r.target, w[2], r.pushedAt, w[3], r.format, r.rows)
	}
	return nil
}

// ---------------------------------------------------------------------------
// push sync --target md:database
// ---------------------------------------------------------------------------

func execPushSync(pa *pushArgs, s *Session) error {
	target, err := openPushTarget(pa, s)
	if err != nil {
		return err
	}
	defer target.close()

	fmt.Printf("Checking sync status against %s...\n", pa.Target)
	anyDrift := false

	for _, entry := range s.Registry {
		for i, rec := range entry.PushRecords {
			if rec.Target != pa.Target {
				continue
			}
			remoteCount, err := target.rowCount(rec.Schema, rec.TableName)
			if err != nil {
				fmt.Printf("  ⚠  %s: could not check remote (%v)\n", entry.Name, err)
				continue
			}
			localCount := entry.RowCount
			if remoteCount == localCount {
				fmt.Printf("  ✓ %s  %d rows  in sync\n", entry.Name, localCount)
			} else {
				anyDrift = true
				fmt.Printf("  ✗ %s  local=%d  remote=%d  drift detected\n",
					entry.Name, localCount, remoteCount)
				if !pa.DryRun {
					rec2, pushErr := target.push(entry, rec.Schema, false, s)
					if pushErr != nil {
						fmt.Printf("    re-push failed: %v\n", pushErr)
					} else {
						entry.PushRecords[i] = *rec2
						fmt.Printf("    re-pushed → %d rows\n", rec2.RowCount)
					}
				}
			}
		}
	}

	if !anyDrift {
		fmt.Println("All pushed tables are in sync.")
	}
	if !pa.DryRun {
		_ = s.saveRegistry()
	}
	return nil
}

// ---------------------------------------------------------------------------
// Interactive warehouse/schema prompt
// ---------------------------------------------------------------------------

// promptWarehouseSchema interactively collects a schema (and, for ZeaDrive
// targets, an optional warehouse path prefix) when neither was provided on the
// CLI nor found in config. It may extend pa.Target with a warehouse segment and
// always sets pa.Schema before returning.
func promptWarehouseSchema(pa *pushArgs) error {
	reader := bufio.NewReader(os.Stdin)
	isZeaDrive := strings.HasPrefix(pa.Target, "zea://")

	warehouse := ""
	schema := "default"

	for {
		fmt.Printf("Push to: %s [Y/n] ", pushPreview(pa.Target, warehouse, schema))
		line, _ := reader.ReadString('\n')
		switch strings.ToLower(strings.TrimSpace(line)) {
		case "", "y":
			if isZeaDrive && warehouse != "" {
				pa.Target = strings.TrimRight(pa.Target, "/") + "/" + warehouse
			}
			pa.Schema = schema
			return nil
		case "n":
			if isZeaDrive {
				fmt.Print("  Warehouse prefix (e.g. 'iceberg', 'prod') [none]: ")
				w, _ := reader.ReadString('\n')
				warehouse = strings.TrimSpace(w)
			}
			fmt.Printf("  Schema [%s]: ", schema)
			s, _ := reader.ReadString('\n')
			if v := strings.TrimSpace(s); v != "" {
				schema = v
			}
		default:
			fmt.Println("  Please enter Y or n.")
		}
	}
}

// pushPreview formats the destination path for display in the prompt.
func pushPreview(target, warehouse, schema string) string {
	if strings.HasPrefix(target, "md:") {
		db := strings.TrimPrefix(target, "md:")
		if db == "" {
			db = "my_db"
		}
		return db + "." + schema
	}
	base := strings.TrimRight(target, "/")
	if warehouse != "" {
		base += "/" + warehouse
	}
	return base + "/" + schema
}

// ---------------------------------------------------------------------------
// Target abstraction
// ---------------------------------------------------------------------------

type pushTarget interface {
	push(entry *TableEntry, schema string, dryRun bool, s *Session) (*PushRecord, error)
	rowCount(schema, table string) (int64, error)
	close()
}

func openPushTarget(pa *pushArgs, s *Session) (pushTarget, error) {
	switch {
	case strings.HasPrefix(pa.Target, "md:"):
		return openMotherDuckTarget(pa.Target, s)
	case strings.HasPrefix(pa.Target, "zea://"):
		return openZeaDriveTarget(pa.Target, pa.Iceberg, s)
	default:
		return nil, fmt.Errorf("push: unsupported target scheme %q — supported: md:, zea://", pa.Target)
	}
}

// ---------------------------------------------------------------------------
// MotherDuck target
// ---------------------------------------------------------------------------

type motherDuckTarget struct {
	db       *sql.DB
	database string // the MotherDuck database name
}

func openMotherDuckTarget(target string, s *Session) (*motherDuckTarget, error) {
	// target is "md:database_name" or "md:" (default database)
	database := strings.TrimPrefix(target, "md:")

	// Resolve token: check ZeaOS token store first, then env.
	token := resolveMotherDuckToken()

	var dsn string
	if token != "" {
		dsn = target + "?motherduck_token=" + token
	} else {
		// MotherDuck CLI auth — token embedded in env or browser auth.
		dsn = target
	}

	fmt.Printf("Connecting to MotherDuck (%s)...\n", target)

	// Install and load the motherduck extension via the session's DuckDB.
	// We open a separate sql.DB for the MotherDuck connection so the session
	// DB is not affected.
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("push: MotherDuck connect: %w", err)
	}

	// Verify connection.
	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("push: MotherDuck ping failed: %w\n"+
			"  Ensure MOTHERDUCK_TOKEN is set or pass --token", err)
	}

	if database == "" {
		database = "my_db"
	}

	fmt.Printf("Connected to MotherDuck.\n")
	return &motherDuckTarget{db: db, database: database}, nil
}

func (t *motherDuckTarget) push(entry *TableEntry, schema string, dryRun bool, s *Session) (*PushRecord, error) {
	ctx := context.Background()

	tableName := entry.Name
	fullTarget := t.database + "." + schema + "." + tableName

	if dryRun {
		return &PushRecord{
			Target:    "md:" + t.database,
			Schema:    schema,
			TableName: tableName,
			PushedAt:  time.Now(),
			RowCount:  entry.RowCount,
			SourceURI: entry.SourceURI,
		}, nil
	}

	// Ensure the table is spilled to Parquet so MotherDuck can read it.
	if err := s.ensureSpilled(entry); err != nil {
		return nil, fmt.Errorf("spill %s: %w", entry.Name, err)
	}

	// Create schema in MotherDuck database.
	createSchema := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", t.database, schema)
	if _, err := t.db.ExecContext(ctx, createSchema); err != nil {
		return nil, fmt.Errorf("create schema: %w", err)
	}

	// Write table from the local Parquet spill file.
	createTable := fmt.Sprintf(
		"CREATE OR REPLACE TABLE %s AS SELECT * FROM read_parquet('%s')",
		fullTarget, entry.FilePath)
	if _, err := t.db.ExecContext(ctx, createTable); err != nil {
		return nil, fmt.Errorf("create table %s: %w", fullTarget, err)
	}

	// Verify row count.
	var pushed int64
	row := t.db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", fullTarget))
	_ = row.Scan(&pushed)

	return &PushRecord{
		Target:    "md:" + t.database,
		Schema:    schema,
		TableName: tableName,
		PushedAt:  time.Now(),
		RowCount:  pushed,
		SourceURI: entry.SourceURI,
	}, nil
}

func (t *motherDuckTarget) rowCount(schema, table string) (int64, error) {
	q := fmt.Sprintf("SELECT count(*) FROM %s.%s.%s", t.database, schema, table)
	var n int64
	err := t.db.QueryRowContext(context.Background(), q).Scan(&n)
	return n, err
}

func (t *motherDuckTarget) close() {
	if t.db != nil {
		t.db.Close()
	}
}

// resolveMotherDuckToken returns a MotherDuck token if one can be found without
// user interaction. Resolution order:
//  1. MOTHERDUCK_TOKEN environment variable
//  2. ~/.motherduck/token (written by browser OAuth on first DuckDB connection)
//
// If neither is present, the caller passes an empty token and the motherduck
// DuckDB extension triggers browser-based OAuth automatically on first connect.
// streamCopyWithProgress copies src to dst using buffered streaming I/O,
// printing periodic progress so a slow FUSE/S3 write doesn't appear hung.
func streamCopyWithProgress(src, dst, label string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return err
	}
	total := info.Size()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	const chunkSize = 4 * 1024 * 1024 // 4 MB
	buf := make([]byte, chunkSize)
	var written int64
	lastPct := -1

	for {
		n, readErr := in.Read(buf)
		if n > 0 {
			if _, writeErr := out.Write(buf[:n]); writeErr != nil {
				return writeErr
			}
			written += int64(n)
			if total > 0 {
				pct := int(written * 100 / total)
				if pct/10 != lastPct/10 { // print every 10%
					fmt.Printf("    %s  %d%%  (%d / %d MB)\n",
						label, pct, written>>20, total>>20)
					lastPct = pct
				}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	// Sync is a best-effort flush — FUSE/S3 backends (e.g. Volumez) do not
	// support fsync and return "operation not supported". Ignore that error;
	// Volumez manages the S3 upload independently of the syscall.
	err = out.Sync()
	if err != nil && !strings.Contains(err.Error(), "operation not supported") {
		return err
	}
	return nil
}

func resolveMotherDuckToken() string {
	if v := getEnv("MOTHERDUCK_TOKEN"); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	tokenPath := filepath.Join(home, ".motherduck", "token")
	data, err := os.ReadFile(tokenPath)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// ---------------------------------------------------------------------------
// ZeaDrive target
// ---------------------------------------------------------------------------

type zeaDriveTarget struct {
	drive      *DriveManager
	zeaPath    string    // full zea:// target, e.g. "zea://s3-data/exports"
	mountCfg   *volMount // mount config for S3 URI derivation; nil for local-only ZeaDrive
	session    *Session  // used for row count queries during sync
	useIceberg bool      // write Iceberg table format instead of flat Parquet
}

func openZeaDriveTarget(target string, iceberg bool, s *Session) (*zeaDriveTarget, error) {
	if err := s.Drive.EnsureCloudMount(); err != nil {
		return nil, fmt.Errorf("push: ZeaDrive not available: %w", err)
	}
	// If FUSE is not mounted, SDK mode is implied — EnsureCloudMount already
	// returned nil. Check that we at least have an S3 backend configured so
	// the expanded path will be resolvable.
	if !s.Drive.IsMounted() {
		expanded := s.Drive.ExpandPath(target)
		if !s.Drive.IsS3Path(expanded) {
			return nil, fmt.Errorf("push: ZeaDrive is not mounted and no S3 backend matches %q — run 'zeadrive mount' or 'enable-s3'", target)
		}
	}

	// Find the matching mount config for S3 URI derivation.
	rest := strings.TrimPrefix(target, "zea://")
	backendName := strings.SplitN(rest, "/", 2)[0]

	cfg, _ := s.Drive.loadConfig()
	var mountCfg *volMount
	if cfg != nil {
		for i, m := range cfg.Mounts {
			if strings.TrimPrefix(m.Path, "/") == backendName {
				mountCfg = &cfg.Mounts[i]
				break
			}
		}
	}
	if backendName != "" && mountCfg == nil {
		return nil, fmt.Errorf("push: no ZeaDrive backend named %q — run 'zeadrive status' to list configured backends", backendName)
	}

	return &zeaDriveTarget{
		drive:      s.Drive,
		zeaPath:    target,
		mountCfg:   mountCfg,
		session:    s,
		useIceberg: iceberg,
	}, nil
}

func (t *zeaDriveTarget) push(entry *TableEntry, schema string, dryRun bool, s *Session) (*PushRecord, error) {
	if err := s.ensureSpilled(entry); err != nil {
		return nil, fmt.Errorf("spill %s: %w", entry.Name, err)
	}

	s3URI := t.deriveS3URI(schema, entry.Name)

	if dryRun {
		return &PushRecord{
			Target:    t.zeaPath,
			Schema:    schema,
			TableName: entry.Name,
			PushedAt:  time.Now(),
			RowCount:  entry.RowCount,
			SourceURI: entry.SourceURI,
			S3URI:     s3URI,
		}, nil
	}

	if t.useIceberg {
		return t.pushIceberg(entry, schema, s3URI, s)
	}
	return t.pushParquet(entry, schema, s3URI)
}

func (t *zeaDriveTarget) pushParquet(entry *TableEntry, schema, s3URI string) (*PushRecord, error) {
	destZeaPath := t.zeaPath + "/" + schema + "/" + entry.Name + ".parquet"
	destFSPath := t.drive.ExpandPath(destZeaPath)

	if t.drive.IsS3Path(destFSPath) {
		// SDK mode: upload via S3 SDK directly.
		data, err := os.ReadFile(entry.FilePath)
		if err != nil {
			return nil, fmt.Errorf("read parquet: %w", err)
		}
		if err := t.drive.WriteFile(destZeaPath, data); err != nil {
			return nil, fmt.Errorf("upload to S3: %w", err)
		}
	} else {
		if err := os.MkdirAll(filepath.Dir(destFSPath), 0o755); err != nil {
			return nil, fmt.Errorf("mkdir: %w", err)
		}
		if err := streamCopyWithProgress(entry.FilePath, destFSPath, entry.Name); err != nil {
			return nil, fmt.Errorf("copy to ZeaDrive: %w", err)
		}
	}
	return &PushRecord{
		Target:    t.zeaPath,
		Schema:    schema,
		TableName: entry.Name,
		PushedAt:  time.Now(),
		RowCount:  entry.RowCount,
		SourceURI: entry.SourceURI,
		S3URI:     s3URI,
	}, nil
}

func (t *zeaDriveTarget) pushIceberg(entry *TableEntry, schema, s3URI string, s *Session) (*PushRecord, error) {
	arrowSchema, err := s.arrowSchemaForEntry(entry)
	if err != nil {
		return nil, fmt.Errorf("get arrow schema for %s: %w", entry.Name, err)
	}

	lineage := buildLineageInfo(s, entry)

	// Stage the Iceberg table locally — Volumez FUSE doesn't support small
	// random-write files (manifests, version-hint.text) directly. Build the
	// complete table under ~/.zeaos/iceberg-staging/<name>/, then copy the
	// tree to ZeaDrive in a single streaming pass.
	stagingDir := filepath.Join(s.Dir, "iceberg-staging", schema, entry.Name)
	if err := os.RemoveAll(stagingDir); err != nil {
		return nil, fmt.Errorf("clear staging dir: %w", err)
	}

	tableZeaPath := t.zeaPath + "/" + schema + "/" + entry.Name
	tableFSPath := t.drive.ExpandPath(tableZeaPath)

	// Register the data file as a zea:// path in the manifest so any ZeaOS
	// user (not just the pusher) can resolve it via their own ExpandPath.
	snapshotID := time.Now().UnixMilli()
	dataZeaPath := tableZeaPath + "/data/" + fmt.Sprintf("%d.parquet", snapshotID)
	dataFSPath := t.drive.ExpandPath(dataZeaPath)

	var tbl *zeaberg.Table
	if icebergTableExists(tableFSPath, t.drive) {
		// Append mode: stage existing metadata locally so zeaberg can open the
		// table, then append a new snapshot rather than recreating it.
		if err := os.MkdirAll(filepath.Join(stagingDir, "metadata"), 0o755); err != nil {
			return nil, fmt.Errorf("create staging metadata dir: %w", err)
		}
		if err := stageExistingIcebergMetadata(stagingDir, tableFSPath, t.drive); err != nil {
			return nil, fmt.Errorf("stage existing iceberg metadata: %w", err)
		}
		tbl, err = zeaberg.OpenTable(stagingDir)
		if err != nil {
			return nil, fmt.Errorf("iceberg open (staging): %w", err)
		}
	} else {
		tbl, err = zeaberg.CreateTable(stagingDir, arrowSchema,
			zeaberg.WithCanonicalLocation(tableFSPath),
		)
		if err != nil {
			return nil, fmt.Errorf("iceberg create (staging): %w", err)
		}
	}

	// Rewrite the Parquet file with Iceberg field IDs embedded before registering
	// it. DuckDB's iceberg_scan matches columns by field ID; without them every
	// column reads as NULL. The rewritten file also has the correct size for the
	// manifest entry.
	rewrittenParquet := filepath.Join(stagingDir, "data", "data.parquet")
	if err := os.MkdirAll(filepath.Dir(rewrittenParquet), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir rewrite: %w", err)
	}
	if err := zeaberg.RewriteWithFieldIDs(entry.FilePath, rewrittenParquet); err != nil {
		return nil, fmt.Errorf("rewrite parquet field IDs: %w", err)
	}

	// Build metadata locally with externalPath — no data copy into staging.
	// Store the FUSE-resolved path so DuckDB and other Iceberg readers can open
	// the file directly. The zea:// path is retained in the snapshot summary via
	// zea.data_file for ZeaOS internal tracking.
	if err := tbl.AppendSnapshot(rewrittenParquet, entry.RowCount,
		zeaberg.WithLineage(lineage),
		zeaberg.WithExternalPath(dataFSPath),
		zeaberg.WithZeaDataPath(dataZeaPath),
	); err != nil {
		return nil, fmt.Errorf("iceberg append snapshot: %w", err)
	}

	// Copy metadata tree (small files) to ZeaDrive via S3 SDK (bypasses FUSE
	// for reliable small-file writes on backends like Volumez).
	if err := t.drive.CopyDirToMount(stagingDir, tableZeaPath); err != nil {
		return nil, fmt.Errorf("copy iceberg metadata to ZeaDrive: %w", err)
	}

	// Stream the rewritten Parquet data file to its registered path on ZeaDrive.
	if t.drive.IsS3Path(dataFSPath) {
		// SDK mode: read and upload via S3 SDK.
		data, err := os.ReadFile(rewrittenParquet)
		if err != nil {
			return nil, fmt.Errorf("read rewritten parquet: %w", err)
		}
		if err := t.drive.WriteFile(dataZeaPath, data); err != nil {
			return nil, fmt.Errorf("upload parquet to S3: %w", err)
		}
	} else {
		if err := os.MkdirAll(filepath.Dir(dataFSPath), 0o755); err != nil {
			return nil, fmt.Errorf("mkdir data: %w", err)
		}
		if err := streamCopyWithProgress(rewrittenParquet, dataFSPath, entry.Name); err != nil {
			return nil, fmt.Errorf("stream parquet to ZeaDrive: %w", err)
		}
	}

	return &PushRecord{
		Target:    t.zeaPath,
		Schema:    schema,
		TableName: entry.Name,
		PushedAt:  time.Now(),
		RowCount:  entry.RowCount,
		SourceURI: entry.SourceURI,
		S3URI:     s3URI,
		Format:    "iceberg",
	}, nil
}


// icebergTableExists reports whether an Iceberg table already exists at tableFSPath.
// In SDK mode tableFSPath is an s3:// URI; in FUSE/local mode it is a filesystem path.
func icebergTableExists(tableFSPath string, drive *DriveManager) bool {
	if drive.IsS3Path(tableFSPath) {
		_, err := drive.ReadS3Path(tableFSPath + "/metadata/version-hint.text")
		return err == nil
	}
	_, err := os.Stat(filepath.Join(tableFSPath, "metadata", "version-hint.text"))
	return err == nil
}

// stageExistingIcebergMetadata downloads version-hint.text and the current
// metadata JSON from an existing Iceberg table into stagingDir so that
// zeaberg.OpenTable can read the table state from local files.
func stageExistingIcebergMetadata(stagingDir, tableFSPath string, drive *DriveManager) error {
	readFile := func(remotePath string) ([]byte, error) {
		if drive.IsS3Path(tableFSPath) {
			return drive.ReadS3Path(remotePath)
		}
		return os.ReadFile(remotePath)
	}
	remotePath := func(rel string) string {
		if drive.IsS3Path(tableFSPath) {
			return tableFSPath + "/" + rel
		}
		return filepath.Join(tableFSPath, filepath.FromSlash(rel))
	}

	hintData, err := readFile(remotePath("metadata/version-hint.text"))
	if err != nil {
		return fmt.Errorf("read version-hint.text: %w", err)
	}
	if err := os.WriteFile(filepath.Join(stagingDir, "metadata", "version-hint.text"), hintData, 0644); err != nil {
		return err
	}

	versionStr := strings.TrimSpace(string(hintData))
	metaFile := "v" + versionStr + ".metadata.json"
	metaData, err := readFile(remotePath("metadata/" + metaFile))
	if err != nil {
		return fmt.Errorf("read %s: %w", metaFile, err)
	}
	return os.WriteFile(filepath.Join(stagingDir, "metadata", metaFile), metaData, 0644)
}

// buildLineageInfo constructs a zeaberg.LineageInfo from the session entry's
// lineage chain, embedding provenance that travels with the Iceberg snapshot.
func buildLineageInfo(s *Session, entry *TableEntry) *zeaberg.LineageInfo {
	info := &zeaberg.LineageInfo{
		SessionID:  s.Dir,
		SourceURIs: nil,
	}

	chain, err := walkLineage(s, entry.Name)
	if err == nil {
		info.SourceURIs = chain.SourceURIs
		for _, node := range chain.Nodes {
			info.Chain = append(info.Chain, zeaberg.ChainEntry{
				Name:      node.Entry.Name,
				Operation: node.NodeKind,
				SourceURI: node.Entry.SourceURI,
			})
		}
	}

	// Resolve PromotedAs from the promotions map.
	for exportName, art := range s.Promoted {
		if art.PromotedFrom == entry.Name {
			info.PromotedAs = exportName
			break
		}
	}

	return info
}

// deriveS3URI constructs the canonical s3://bucket/key URI for a pushed file
// using the mount config. Returns empty string for non-S3 backends.
func (t *zeaDriveTarget) deriveS3URI(schema, tableName string) string {
	if t.mountCfg == nil || t.mountCfg.Backend != "s3" {
		return ""
	}
	bucket, _ := t.mountCfg.Config["bucket"].(string)
	if bucket == "" {
		return ""
	}
	prefix, _ := t.mountCfg.Config["prefix"].(string)

	// Sub-path after the backend name in the zea:// URL.
	rest := strings.TrimPrefix(t.zeaPath, "zea://")
	parts := strings.SplitN(rest, "/", 2)
	var subPath string
	if len(parts) > 1 && parts[1] != "" {
		subPath = parts[1] + "/"
	}

	key := strings.Join([]string{prefix, subPath + schema + "/" + tableName + ".parquet"}, "/")
	key = strings.TrimPrefix(strings.ReplaceAll(key, "//", "/"), "/")
	return "s3://" + bucket + "/" + key
}

func (t *zeaDriveTarget) rowCount(schema, table string) (int64, error) {
	destZeaPath := t.zeaPath + "/" + schema + "/" + table + ".parquet"
	destFSPath := t.drive.ExpandPath(destZeaPath)

	if _, err := os.Stat(destFSPath); err != nil {
		return 0, fmt.Errorf("not found at %s", destZeaPath)
	}

	var n int64
	err := t.session.db.QueryRow(
		fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')", sqlEsc(destFSPath)),
	).Scan(&n)
	return n, err
}

func (t *zeaDriveTarget) close() {}

