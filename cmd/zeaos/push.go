package main

import (
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
	pa := &pushArgs{Schema: "zea_exports"}

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
		if cfg != nil && cfg.Push.DefaultTarget != "" {
			pa.Target = cfg.Push.DefaultTarget
		}
	}

	if pa.Target == "" && pa.Subcommand != "status" {
		return nil, fmt.Errorf("push: --target required (or set a default with push --target md:db --set-default)")
	}

	// Save as default if first explicit use.
	if pa.Target != "" {
		cfg, _ := loadConfig()
		if cfg == nil {
			cfg = &zeaosConfig{}
		}
		if cfg.Push.DefaultTarget != pa.Target {
			cfg.Push.DefaultTarget = pa.Target
			_ = saveConfig(cfg)
		}
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
				if err := updateRepoStagingMacro(pa.Target, sources); err != nil {
					fmt.Printf("  ⚠  could not update repo macro: %v\n", err)
				}
			}
		}
	}
	return nil
}

// resolvePushTables returns the session tables to push.
//
//  1. Explicit names on the command line — push exactly those.
//  2. Promotions present — push source (load-node) tables from their lineage.
//  3. No promotions — push all session tables not prefixed with "_".
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

	// Collect source (load-node) tables from promoted artifact lineage.
	seen := map[string]bool{}
	var out []*TableEntry
	for _, art := range s.Promoted {
		chain, err := walkLineage(s, art.PromotedFrom)
		if err != nil {
			continue
		}
		for _, node := range chain.Nodes {
			if node.NodeKind == "load" && !seen[node.Entry.Name] {
				seen[node.Entry.Name] = true
				out = append(out, node.Entry)
			}
		}
	}
	if len(out) > 0 {
		return out, nil
	}

	// No promotions — fall back to all non-internal session tables.
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
	any := false
	fmt.Printf("%-24s  %-32s  %-20s  %s\n", "Table", "Target", "Pushed At", "Rows")
	fmt.Println(strings.Repeat("─", 90))
	for _, entry := range s.Registry {
		for _, rec := range entry.PushRecords {
			any = true
			fmt.Printf("%-24s  %-32s  %-20s  %d\n",
				entry.Name,
				rec.Target+"/"+rec.Schema+"."+rec.TableName,
				rec.PushedAt.Format("2006-01-02 15:04:05"),
				rec.RowCount)
		}
	}
	if !any {
		fmt.Println("No push history. Use 'push --target md:database' to push session tables.")
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
	if !s.Drive.IsMounted() {
		return nil, fmt.Errorf("push: ZeaDrive is not mounted — run 'zeadrive mount' first")
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

	if err := os.MkdirAll(filepath.Dir(destFSPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir: %w", err)
	}
	if err := streamCopyWithProgress(entry.FilePath, destFSPath, entry.Name); err != nil {
		return nil, fmt.Errorf("copy to ZeaDrive: %w", err)
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
	tbl, err = zeaberg.CreateTable(stagingDir, arrowSchema,
		zeaberg.WithCanonicalLocation(tableFSPath),
	)
	if err != nil {
		return nil, fmt.Errorf("iceberg create (staging): %w", err)
	}

	// Build metadata locally with externalPath — no data copy into staging.
	// Store the zea:// path so any ZeaOS user can resolve it, not just the pusher.
	if err := tbl.AppendSnapshot(entry.FilePath, entry.RowCount,
		zeaberg.WithLineage(lineage),
		zeaberg.WithExternalPath(dataZeaPath),
	); err != nil {
		return nil, fmt.Errorf("iceberg append snapshot: %w", err)
	}

	// Copy metadata tree (small files) to ZeaDrive via S3 SDK (bypasses FUSE
	// for reliable small-file writes on backends like Volumez).
	if err := t.drive.CopyDirToMount(stagingDir, tableZeaPath); err != nil {
		return nil, fmt.Errorf("copy iceberg metadata to ZeaDrive: %w", err)
	}

	// Stream the Parquet data file directly to its registered path on ZeaDrive.
	if err := os.MkdirAll(filepath.Dir(dataFSPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir data: %w", err)
	}
	if err := streamCopyWithProgress(entry.FilePath, dataFSPath, entry.Name); err != nil {
		return nil, fmt.Errorf("stream parquet to ZeaDrive: %w", err)
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

