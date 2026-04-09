package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/gdamore/tcell/v2"
	duckdb "github.com/marcboeker/go-duckdb/v2"
	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/rivo/tview"
)

// TableEntry holds metadata for a session table.
// Exported fields are persisted to session.json; runtime Arrow fields are not.
type TableEntry struct {
	Name      string    `json:"name"`
	FilePath  string    `json:"file_path"` // Parquet spill path for persistence
	RowCount  int64     `json:"row_count"`
	ColCount  int       `json:"col_count"`
	Parent    string    `json:"parent,omitempty"`
	Ops       []string  `json:"ops,omitempty"`
	CreatedAt time.Time `json:"created_at"`

	// Source tracking for dbt export.
	SourceSQL string `json:"source_sql,omitempty"` // verbatim zeaql query, if applicable
	SourceURI string `json:"source_uri,omitempty"` // original load URI before path expansion

	// Push history — recorded each time this table is pushed to an external target.
	PushRecords []PushRecord `json:"push_records,omitempty"`

	// Runtime-only Arrow state — not serialised.
	records []arrow.Record
	schema  *arrow.Schema
}

// Session holds the live DuckDB Arrow connection, table registry, and session paths.
type Session struct {
	Dir       string
	TablesDir string
	Registry  map[string]*TableEntry
	Promoted  map[string]*PromotedArtifact // keyed by export name
	Drive     *DriveManager

	db        *sql.DB
	arrowConn *sql.Conn
	arrow     *duckdb.Arrow
}

func NewSession() (*Session, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(home, ".zeaos")
	tablesDir := filepath.Join(dir, "tables")
	for _, d := range []string{dir, tablesDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", d, err)
		}
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("duckdb open: %w", err)
	}

	ctx := context.Background()

	// Pin a single connection for all Arrow work. database/sql's *sql.Conn
	// guarantees that every operation on it uses the same underlying driver
	// connection, so RegisterView and QueryContext share the same DuckDB session.
	arrowConn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("db.Conn: %w", err)
	}

	// Obtain the Arrow accessor bound to the pinned driver connection.
	var ar *duckdb.Arrow
	if err := arrowConn.Raw(func(c any) error {
		dc, ok := c.(driver.Conn)
		if !ok {
			return fmt.Errorf("unexpected driver type %T", c)
		}
		var err error
		ar, err = duckdb.NewArrowFromConn(dc)
		return err
	}); err != nil {
		arrowConn.Close()
		db.Close()
		return nil, fmt.Errorf("arrow init: %w", err)
	}

	// Install and load DuckDB extensions. INSTALL is a no-op if already
	// present; both steps are non-fatal so a missing network or unsupported
	// platform does not prevent ZeaOS from starting.
	for _, ext := range []string{"iceberg"} {
		_, _ = arrowConn.ExecContext(ctx, "INSTALL "+ext)
		_, _ = arrowConn.ExecContext(ctx, "LOAD "+ext)
	}

	s := &Session{
		Dir:       dir,
		TablesDir: tablesDir,
		Registry:  make(map[string]*TableEntry),
		Promoted:  make(map[string]*PromotedArtifact),
		db:        db,
		arrowConn: arrowConn,
		arrow:     ar,
		Drive:     NewDriveManager(dir),
	}
	_ = s.loadRegistry() // non-fatal: start fresh if no prior session
	return s, nil
}

func (s *Session) Close() {
	ctx := context.Background()
	_ = s.spillAll(ctx)
	_ = s.saveRegistry()
	for _, e := range s.Registry {
		for _, r := range e.records {
			r.Release()
		}
	}
	_ = s.arrowConn.Close()
	_ = s.db.Close()
	s.Drive.Stop()
}

// TablePath returns the canonical Parquet spill path for a named table.
func (s *Session) TablePath(name string) string {
	return filepath.Join(s.TablesDir, name+".parquet")
}

// Get returns a table entry or an error if not found.
func (s *Session) Get(name string) (*TableEntry, error) {
	e, ok := s.Registry[name]
	if !ok {
		return nil, fmt.Errorf("table %q not found", name)
	}
	return e, nil
}

// Drop removes a table from the registry, releases its Arrow records, and
// deletes its managed Parquet spill file.
func (s *Session) Drop(name string) error {
	entry, err := s.Get(name)
	if err != nil {
		return err
	}
	s.dropEntry(entry)
	delete(s.Registry, name)
	return nil
}

// dropEntry releases Arrow records and removes the spill file for an entry.
func (s *Session) dropEntry(e *TableEntry) {
	for _, r := range e.records {
		r.Release()
	}
	e.records = nil
	if e.FilePath != "" && filepath.Dir(e.FilePath) == s.TablesDir {
		_ = os.Remove(e.FilePath)
	}
}

// storeArrow registers pre-filtered Arrow records directly into the session
// registry without executing a DuckDB query. Used by the Arrow-side where
// filter path so that DuckDB never sees the predicate.
func (s *Session) storeArrow(target string, records []arrow.Record, schema *arrow.Schema, parent string, ops []string) (*TableEntry, error) {
	if old, ok := s.Registry[target]; ok {
		s.dropEntry(old)
		delete(s.Registry, target)
	}
	colCount := 0
	if schema != nil {
		colCount = schema.NumFields()
	}
	entry := &TableEntry{
		Name:      target,
		FilePath:  s.TablePath(target),
		RowCount:  recordRowCount(records),
		ColCount:  colCount,
		Parent:    parent,
		Ops:       ops,
		CreatedAt: time.Now(),
		records:   records,
		schema:    schema,
	}
	s.Registry[target] = entry
	return entry, nil
}

// materializeArrow executes query via DuckDB's Arrow interface, stores the
// result as retained Arrow records, and registers it in the session registry.
//
// srcs lists the session table names referenced by query. Each is registered
// as a transient Arrow scan before the query runs and torn down after the
// result is fully collected — so DuckDB reads Arrow memory directly with no
// Parquet round-trip.
func (s *Session) materializeArrow(target, query string, srcs []string, parent string, ops []string) (*TableEntry, error) {
	ctx := context.Background()

	// Replace any existing target entry.
	if old, ok := s.Registry[target]; ok {
		s.dropEntry(old)
		delete(s.Registry, target)
	}

	// Register each source table as a transient Arrow scan + SQL VIEW.
	type reg struct {
		src     string
		scanName string
		release  func()
	}
	var regs []reg

	for _, src := range srcs {
		entry, ok := s.Registry[src]
		if !ok || entry.records == nil {
			continue
		}
		sn := scanID()
		rdr := newRecordSliceReader(entry.schema, entry.records)
		release, err := s.arrow.RegisterView(rdr, sn)
		if err != nil {
			for _, r := range regs {
				r.release()
			}
			return nil, fmt.Errorf("register %q: %w", src, err)
		}
		regs = append(regs, reg{src: src, scanName: sn, release: release})

		// Expose under the user-facing name so the query can reference it naturally.
		_, _ = s.arrowConn.ExecContext(ctx, fmt.Sprintf(`DROP VIEW IF EXISTS %q`, src))
		_, _ = s.arrowConn.ExecContext(ctx, fmt.Sprintf(`CREATE VIEW %q AS SELECT * FROM %q`, src, sn))
	}

	// Execute the query via the Arrow interface.
	rdr, queryErr := s.arrow.QueryContext(ctx, query)

	// Fully collect the result before releasing source views — DuckDB may
	// evaluate lazily and still need the sources during Next() calls.
	var schema *arrow.Schema
	var records []arrow.Record
	if queryErr == nil {
		schema = rdr.Schema()
		for rdr.Next() {
			rec := rdr.Record()
			rec.Retain()
			records = append(records, rec)
		}
		if err := rdr.Err(); err != nil {
			for _, r := range records {
				r.Release()
			}
			records = nil
			queryErr = err
		}
		rdr.Release()
	}

	// Tear down transient source views and release their C streams.
	for _, r := range regs {
		r.release()
		_, _ = s.arrowConn.ExecContext(ctx, fmt.Sprintf(`DROP VIEW IF EXISTS %q`, r.src))
	}

	if queryErr != nil {
		return nil, queryErr
	}

	colCount := 0
	if schema != nil {
		colCount = schema.NumFields()
	}

	entry := &TableEntry{
		Name:      target,
		FilePath:  s.TablePath(target),
		RowCount:  recordRowCount(records),
		ColCount:  colCount,
		Parent:    parent,
		Ops:       ops,
		CreatedAt: time.Now(),
		records:   records,
		schema:    schema,
	}
	s.Registry[target] = entry
	return entry, nil
}

// materializeViaTable executes an aggregation query (GROUP BY, PIVOT) safely
// by first copying the source Arrow records into a native DuckDB in-memory
// table, running the aggregation against that, then reading the result back
// as Arrow.
//
// DuckDB v1.8.5 crashes (SIGSEGV in duckdb_execute_prepared_arrow) when any
// query — Arrow or regular SQL — performs GROUP BY or PIVOT over an Arrow C
// Data Interface scan (RegisterView). The Arrow stream callbacks are called
// from DuckDB's C++ GROUP BY executor in a way that corrupts state. Copying
// source records into a native DuckDB table first avoids this entirely.
// Only the source records for the aggregation are copied; the result is read
// back zero-copy as Arrow.
func (s *Session) materializeViaTable(target, query string, srcs []string, parent string, ops []string) (*TableEntry, error) {
	ctx := context.Background()

	if old, ok := s.Registry[target]; ok {
		s.dropEntry(old)
		delete(s.Registry, target)
	}

	// Copy each source's Arrow records into a native DuckDB in-memory table
	// so the aggregation query reads DuckDB-native data, not an Arrow C stream.
	type srcTable struct{ name, tmp string }
	var srcTables []srcTable

	for _, src := range srcs {
		entry, ok := s.Registry[src]
		if !ok || entry.records == nil {
			continue
		}
		tmp := "_zsrc_" + src
		_, _ = s.arrowConn.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %q`, tmp))

		// Register Arrow stream, create VIEW, copy into native table, tear down.
		sn := scanID()
		rdr := newRecordSliceReader(entry.schema, entry.records)
		release, err := s.arrow.RegisterView(rdr, sn)
		if err != nil {
			for _, st := range srcTables {
				_, _ = s.arrowConn.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %q`, st.tmp))
			}
			return nil, fmt.Errorf("register %q: %w", src, err)
		}
		_, copyErr := s.arrowConn.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %q AS SELECT * FROM %q`, tmp, sn))
		release()
		if copyErr != nil {
			for _, st := range srcTables {
				_, _ = s.arrowConn.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %q`, st.tmp))
				_, _ = s.arrowConn.ExecContext(ctx, fmt.Sprintf(`DROP VIEW IF EXISTS %q`, st.name))
			}
			return nil, fmt.Errorf("copy %q to table: %w", src, copyErr)
		}

		// Expose under the user-facing name so unquoted references in zeaql
		// queries (e.g. FROM trips) resolve to the native table, not the Arrow stream.
		_, _ = s.arrowConn.ExecContext(ctx, fmt.Sprintf(`DROP VIEW IF EXISTS %q`, src))
		_, _ = s.arrowConn.ExecContext(ctx, fmt.Sprintf(`CREATE VIEW %q AS SELECT * FROM %q`, src, tmp))

		srcTables = append(srcTables, srcTable{name: src, tmp: tmp})
	}

	// Rewrite the query to reference native table names directly, avoiding
	// the need to create VIEWs whose catalog visibility was unreliable.
	nativeQuery := query
	for _, st := range srcTables {
		nativeQuery = strings.ReplaceAll(nativeQuery, `"`+st.name+`"`, `"`+st.tmp+`"`)
	}

	// Run the aggregation against native DuckDB tables.
	tmpResult := "_ztbl_" + target
	_, _ = s.arrowConn.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %q`, tmpResult))
	_, execErr := s.arrowConn.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE %q AS %s`, tmpResult, nativeQuery))

	// Tear down source temp tables and their user-facing views.
	for _, st := range srcTables {
		_, _ = s.arrowConn.ExecContext(ctx, fmt.Sprintf(`DROP VIEW IF EXISTS %q`, st.name))
		_, _ = s.arrowConn.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %q`, st.tmp))
	}

	if execErr != nil {
		_, _ = s.arrowConn.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %q`, tmpResult))
		return nil, execErr
	}

	// Read the aggregated result back as Arrow (simple scan — no Arrow stream involved).
	rdr, err := s.arrow.QueryContext(ctx, fmt.Sprintf(`SELECT * FROM %q`, tmpResult))
	_, _ = s.arrowConn.ExecContext(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %q`, tmpResult))
	if err != nil {
		return nil, err
	}

	schema := rdr.Schema()
	var records []arrow.Record
	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()
		records = append(records, rec)
	}
	if err := rdr.Err(); err != nil {
		for _, r := range records {
			r.Release()
		}
		rdr.Release()
		return nil, err
	}
	rdr.Release()

	colCount := 0
	if schema != nil {
		colCount = schema.NumFields()
	}

	entry := &TableEntry{
		Name:      target,
		FilePath:  s.TablePath(target),
		RowCount:  recordRowCount(records),
		ColCount:  colCount,
		Parent:    parent,
		Ops:       ops,
		CreatedAt: time.Now(),
		records:   records,
		schema:    schema,
	}
	s.Registry[target] = entry
	return entry, nil
}

// SaveTable writes a table's Arrow records to a user-specified path.
// Format is determined by ext: .parquet, .csv, .json, .jsonl; anything else
// defaults to CSV. Uses DuckDB COPY so the Arrow scan predicate pushdown bug
// is not a concern (COPY reads all rows unconditionally).
func (s *Session) SaveTable(entry *TableEntry, path, ext string) error {
	ctx := context.Background()
	sn := scanID()
	rdr := newRecordSliceReader(entry.schema, entry.records)
	release, err := s.arrow.RegisterView(rdr, sn)
	if err != nil {
		return fmt.Errorf("save: %w", err)
	}
	defer release()

	var copySQL string
	switch ext {
	case ".parquet":
		copySQL = fmt.Sprintf(`COPY (SELECT * FROM %q) TO '%s' (FORMAT PARQUET)`, sn, sqlEsc(path))
	case ".csv":
		copySQL = fmt.Sprintf(`COPY (SELECT * FROM %q) TO '%s' (FORMAT CSV, HEADER)`, sn, sqlEsc(path))
	case ".json", ".jsonl":
		copySQL = fmt.Sprintf(`COPY (SELECT * FROM %q) TO '%s' (FORMAT JSON)`, sn, sqlEsc(path))
	default:
		copySQL = fmt.Sprintf(`COPY (SELECT * FROM %q) TO '%s' (FORMAT CSV, HEADER)`, sn, sqlEsc(path))
	}

	_, err = s.arrowConn.ExecContext(ctx, copySQL)
	return err
}

// spillOne writes a single table's Arrow records to its Parquet spill file.
// It is a no-op if the file already exists or the entry has no in-memory records.
// COPY reads all rows unconditionally so it is not affected by the DuckDB
// Arrow scan integer-predicate pushdown bug.
func (s *Session) spillOne(ctx context.Context, entry *TableEntry) error {
	if entry.records == nil || entry.schema == nil {
		return nil
	}
	if _, err := os.Stat(entry.FilePath); err == nil {
		return nil // already on disk
	}
	sn := scanID()
	rdr := newRecordSliceReader(entry.schema, entry.records)
	release, err := s.arrow.RegisterView(rdr, sn)
	if err != nil {
		return err
	}
	defer release()
	_, err = s.arrowConn.ExecContext(ctx,
		fmt.Sprintf(`COPY (SELECT * FROM %q) TO '%s' (FORMAT PARQUET)`, sn, sqlEsc(entry.FilePath)))
	return err
}

// spillAll writes every in-memory table to its Parquet spill file for
// session persistence. Called during Close().
func (s *Session) spillAll(ctx context.Context) error {
	for _, entry := range s.Registry {
		_ = s.spillOne(ctx, entry)
	}
	return nil
}

// ensureSpilled guarantees a table's Parquet file is present on disk.
// Called by push before handing the file path to an external target.
func (s *Session) ensureSpilled(entry *TableEntry) error {
	return s.spillOne(context.Background(), entry)
}

// arrowSchemaForEntry returns the Arrow schema for an entry. Uses the
// runtime schema if already populated, otherwise reads it from the spilled
// Parquet file via DuckDB's Arrow interface.
func (s *Session) arrowSchemaForEntry(entry *TableEntry) (*arrow.Schema, error) {
	if entry.schema != nil {
		return entry.schema, nil
	}
	if entry.FilePath == "" {
		return nil, fmt.Errorf("no spill file for %q", entry.Name)
	}
	// Execute a zero-row Arrow query to get the schema from DuckDB.
	rdr, err := s.arrow.QueryContext(context.Background(),
		fmt.Sprintf("SELECT * FROM read_parquet('%s') LIMIT 0", sqlEsc(entry.FilePath)))
	if err != nil {
		return nil, fmt.Errorf("arrow schema query: %w", err)
	}
	defer rdr.Release()
	return rdr.Schema(), nil
}

// getEnv returns the value of the named environment variable.
func getEnv(key string) string {
	// os.Getenv is the canonical way; wrapping here keeps push.go import-free.
	return os.Getenv(key)
}

// sessionData is the persisted envelope for session.json.
type sessionData struct {
	Version  string                       `json:"version"`
	Registry map[string]*TableEntry       `json:"registry"`
	Promoted map[string]*PromotedArtifact `json:"promoted,omitempty"`
}

func (s *Session) saveRegistry() error {
	data, err := json.MarshalIndent(sessionData{
		Version:  "0.2.0",
		Registry: s.Registry,
		Promoted: s.Promoted,
	}, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.Dir, "session.json"), data, 0o644)
}

func (s *Session) loadRegistry() error {
	ctx := context.Background()
	data, err := os.ReadFile(filepath.Join(s.Dir, "session.json"))
	if err != nil {
		return err
	}
	var sd sessionData
	if err := json.Unmarshal(data, &sd); err != nil {
		return err
	}
	reg := sd.Registry
	if reg == nil {
		return fmt.Errorf("session.json: missing registry")
	}
	if sd.Promoted != nil {
		s.Promoted = sd.Promoted
	}

	for name, e := range reg {
		info, err := os.Stat(e.FilePath)
		if err != nil || info.IsDir() {
			delete(reg, name)
			continue
		}
		// Reload Parquet → Arrow records.
		rdr, err := s.arrow.QueryContext(ctx,
			fmt.Sprintf("SELECT * FROM read_parquet('%s')", sqlEsc(e.FilePath)))
		if err != nil {
			delete(reg, name)
			continue
		}
		schema := rdr.Schema()
		var records []arrow.Record
		for rdr.Next() {
			rec := rdr.Record()
			rec.Retain()
			records = append(records, rec)
		}
		if err := rdr.Err(); err != nil {
			for _, r := range records {
				r.Release()
			}
			rdr.Release()
			delete(reg, name)
			continue
		}
		rdr.Release()
		e.records = records
		e.schema = schema
	}

	s.Registry = reg
	return nil
}

// ShowStatus displays a TUI panel with session state: tables, drive, and memory.
func (s *Session) ShowStatus() {
	app := tview.NewApplication()

	text := tview.NewTextView().
		SetDynamicColors(true).
		SetScrollable(true).
		SetWordWrap(false)

	var b strings.Builder

	// Memory
	fmt.Fprintf(&b, "[yellow]Memory[-]\n  %s\n\n", memUsage())

	// Drive
	fmt.Fprintf(&b, "[yellow]Drive[-]\n  %s\n\n", s.Drive.Label())

	// Tables
	fmt.Fprintf(&b, "[yellow]Tables[-]  (%d)\n", len(s.Registry))
	if len(s.Registry) == 0 {
		fmt.Fprintf(&b, "  (none)\n")
	} else {
		for _, e := range s.Registry {
			fmt.Fprintf(&b, "  [green]%s[-]  %d rows × %d cols", e.Name, e.RowCount, e.ColCount)
			if len(e.Ops) > 0 {
				fmt.Fprintf(&b, "  → %s", strings.Join(e.Ops, " | "))
			}
			fmt.Fprintf(&b, "\n")
		}
	}

	// Plugins
	fmt.Fprintf(&b, "\n[yellow]Plugins[-]\n  %d installed\n", countPlugins())

	text.SetText(b.String())
	text.SetBorder(true)

	text.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape || event.Rune() == 'q' {
			app.Stop()
		}
		return event
	})

	frame := tview.NewFrame(text).
		AddText("ZeaOS — Session Status  (q/Esc to close)", true, tview.AlignCenter, tcell.ColorWhite)

	if err := app.SetRoot(frame, true).Run(); err != nil {
		fmt.Fprintf(os.Stderr, "status: %v\n", err)
	}
}

// sqlEsc escapes single quotes in paths for DuckDB SQL string literals.
func sqlEsc(path string) string {
	return strings.ReplaceAll(path, "'", "\\'")
}

func memUsage() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return fmt.Sprintf("%dMB", m.Alloc/1024/1024)
}

// ShowHist renders a tview TreeView of the table lineage DAG.
func (s *Session) ShowHist() {
	if len(s.Registry) == 0 {
		fmt.Println("(no tables in session)")
		return
	}

	app := tview.NewApplication()
	root := tview.NewTreeNode("session").SetColor(tcell.ColorYellow)
	tree := tview.NewTreeView().SetRoot(root).SetCurrentNode(root)

	added := map[string]bool{}
	var addNode func(name string, parent *tview.TreeNode)
	addNode = func(name string, parent *tview.TreeNode) {
		if added[name] {
			return
		}
		added[name] = true
		e := s.Registry[name]
		label := fmt.Sprintf("%s  [%d rows × %d cols]", name, e.RowCount, e.ColCount)
		if len(e.Ops) > 0 {
			label += "  → " + strings.Join(e.Ops, " | ")
		}
		node := tview.NewTreeNode(label).SetColor(tcell.ColorGreen)
		parent.AddChild(node)
		for childName, childEntry := range s.Registry {
			if childEntry.Parent == name {
				addNode(childName, node)
			}
		}
	}

	for name, e := range s.Registry {
		if e.Parent == "" {
			addNode(name, root)
		}
	}
	for name := range s.Registry {
		if !added[name] {
			addNode(name, root)
		}
	}

	tree.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEscape || event.Rune() == 'q' {
			app.Stop()
		}
		return event
	})

	frame := tview.NewFrame(tree).
		AddText("ZeaOS — Table Lineage  (q/Esc to close)", true, tview.AlignCenter, tcell.ColorWhite)

	if err := app.SetRoot(frame, true).Run(); err != nil {
		fmt.Fprintf(os.Stderr, "hist: %v\n", err)
	}
}
