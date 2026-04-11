package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/open-tempest-labs/zeashell/tui"
)

// execLine dispatches a parsed Cmd to the appropriate handler.
func execLine(line string, s *Session) error {
	cmd, err := ParseLine(line)
	if err != nil {
		return err
	}
	switch cmd.Type {
	case CmdAssignment:
		return execAssignment(cmd, s)
	case CmdBuiltin:
		return execBuiltin(cmd, s)
	case CmdOSPipe:
		return execOSPipe(cmd.Raw, s)
	}
	return nil
}

// --- Assignment handlers ---

func execAssignment(cmd *Cmd, s *Session) error {
	switch cmd.Source {
	case "load":
		return execLoad(cmd, s)
	case "sql":
		return execSQL(cmd, s)
	case "zearun":
		return execPluginCapture(cmd, s)
	default:
		src, err := s.Get(cmd.Source)
		if err != nil {
			return err
		}
		if len(cmd.Ops) == 0 {
			return execCopy(cmd.Target, src, s)
		}
		return execPipe(cmd, src, s)
	}
}

func execLoad(cmd *Cmd, s *Session) error {
	file := cmd.File
	sourceURI := file // capture raw URI before any expansion

	// HTTP/HTTPS: download to a temp file, then load via DuckDB as normal.
	if strings.HasPrefix(file, "http://") || strings.HasPrefix(file, "https://") {
		return execLoadURL(cmd, s, file, sourceURI)
	}

	file = s.Drive.ExpandPath(file)
	// If the path routed to the FUSE mount, ensure Volumez is running.
	if strings.HasPrefix(file, s.Drive.MountPath) {
		if err := s.Drive.EnsureCloudMount(); err != nil {
			return err
		}
	}
	return execLoadFile(cmd, s, file, filepath.Base(file), sourceURI)
}

// execLoadURL downloads a remote file to a temp path and loads it via DuckDB.
// A 10-minute timeout accommodates large public datasets (e.g. NYC Taxi Parquet ~100 MB).
func execLoadURL(cmd *Cmd, s *Session, url, sourceURI string) error {
	fmt.Printf("downloading %s ...\n", url)
	client := &http.Client{Timeout: 10 * time.Minute}
	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("load %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("load %s: HTTP %s", url, resp.Status)
	}

	ext := strings.ToLower(filepath.Ext(url))
	if ext == "" {
		ext = ".parquet"
	}
	tmp, err := os.CreateTemp("", "zeaos-load-*"+ext)
	if err != nil {
		return fmt.Errorf("load %s: %w", url, err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)

	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		return fmt.Errorf("load %s: %w", url, err)
	}
	tmp.Close()

	label := filepath.Base(strings.TrimRight(url, "/"))
	return execLoadFile(cmd, s, tmpPath, label, sourceURI)
}

func execLoadFile(cmd *Cmd, s *Session, file, label, sourceURI string) error {
	ext := strings.ToLower(filepath.Ext(file))
	var readExpr string
	switch ext {
	case ".parquet":
		readExpr = fmt.Sprintf("SELECT * FROM read_parquet('%s')", sqlEsc(file))
	case ".csv":
		readExpr = fmt.Sprintf("SELECT * FROM read_csv_auto('%s')", sqlEsc(file))
	case ".tsv":
		readExpr = fmt.Sprintf("SELECT * FROM read_csv_auto('%s', delim='\t')", sqlEsc(file))
	case ".json", ".jsonl":
		readExpr = fmt.Sprintf("SELECT * FROM read_json_auto('%s')", sqlEsc(file))
	default:
		readExpr = fmt.Sprintf("SELECT * FROM read_csv_auto('%s')", sqlEsc(file))
	}
	entry, err := s.materializeArrow(cmd.Target, readExpr, nil, "",
		[]string{fmt.Sprintf("load(%s)", label)})
	if err != nil {
		return fmt.Errorf("load %s: %w", label, err)
	}
	entry.SourceURI = sourceURI
	fmt.Printf("→ %s: %d rows × %d cols\n", cmd.Target, entry.RowCount, entry.ColCount)
	return nil
}

func execSQL(cmd *Cmd, s *Session) error {
	// Register all session tables so the user's SQL can reference any of them.
	srcs := make([]string, 0, len(s.Registry))
	for name := range s.Registry {
		srcs = append(srcs, name)
	}

	// GROUP BY and PIVOT over Arrow C streams crash DuckDB (same bug as the
	// pipe path). Route through materializeViaTable so sources are copied to
	// native DuckDB tables before the aggregation runs.
	upper := strings.ToUpper(cmd.RawSQL)
	needsTable := strings.Contains(upper, "GROUP BY") || strings.Contains(upper, "PIVOT")

	parent := inferSQLParent(cmd.RawSQL, srcs)
	ops := sqlOps(cmd.RawSQL)

	var entry *TableEntry
	var err error
	if needsTable {
		entry, err = s.materializeViaTable(cmd.Target, cmd.RawSQL, srcs, parent, ops)
	} else {
		entry, err = s.materializeArrow(cmd.Target, cmd.RawSQL, srcs, parent, ops)
	}
	if err != nil {
		return fmt.Errorf("sql: %w", err)
	}
	entry.SourceSQL = cmd.RawSQL
	fmt.Printf("→ %s: %d rows × %d cols\n", cmd.Target, entry.RowCount, entry.ColCount)
	return nil
}

func execCopy(target string, src *TableEntry, s *Session) error {
	q := fmt.Sprintf(`SELECT * FROM "%s"`, src.Name)
	entry, err := s.materializeArrow(target, q, []string{src.Name}, src.Name, []string{"copy"})
	if err != nil {
		return err
	}
	fmt.Printf("→ %s: %d rows × %d cols\n", target, entry.RowCount, entry.ColCount)
	return nil
}

func execPipe(cmd *Cmd, src *TableEntry, s *Session) error {
	opStrs := make([]string, len(cmd.Ops))
	for i, op := range cmd.Ops {
		opStrs[i] = op.Kind + "(" + op.Args + ")"
	}

	// Split where ops from the rest. Where predicates are evaluated directly
	// against Arrow records to avoid DuckDB's Arrow scan predicate pushdown
	// bug, which silently returns all rows for integer column comparisons.
	var whereOps, otherOps []PipeOp
	for _, op := range cmd.Ops {
		if op.Kind == "where" {
			whereOps = append(whereOps, op)
		} else {
			otherOps = append(otherOps, op)
		}
	}

	activeSrc := src
	if len(whereOps) > 0 {
		filtered, err := filterArrowRecords(src.schema, src.records, whereOps)
		if err != nil {
			return fmt.Errorf("pipe: %w", err)
		}
		if len(otherOps) == 0 || len(filtered) == 0 {
			// Where-only chain, or filter produced 0 rows (remaining ops would
			// also produce 0 rows): store filtered result directly.
			entry, err := s.storeArrow(cmd.Target, filtered, src.schema, src.Name, opStrs)
			if err != nil {
				return fmt.Errorf("pipe: %w", err)
			}
			fmt.Printf("→ %s: %d rows × %d cols\n", cmd.Target, entry.RowCount, entry.ColCount)
			return nil
		}
		// Hand filtered records to DuckDB for remaining ops (group, pivot, etc.)
		// by storing them as a temporary session entry.
		tmp := "_ztmp_" + cmd.Target
		activeSrc, err = s.storeArrow(tmp, filtered, src.schema, src.Name, nil)
		if err != nil {
			return fmt.Errorf("pipe: %w", err)
		}
		defer func() {
			s.dropEntry(activeSrc)
			delete(s.Registry, tmp)
		}()
	}

	q, err := BuildSQLFromView(activeSrc.Name, otherOps)
	if err != nil {
		return err
	}

	// GROUP BY and PIVOT trigger a DuckDB v1.8.5 crash in
	// duckdb_execute_prepared_arrow when the source is an Arrow C stream.
	// Route those through an intermediate in-memory DuckDB table instead.
	needsTable := false
	for _, op := range otherOps {
		if op.Kind == "group" || op.Kind == "pivot" {
			needsTable = true
			break
		}
	}

	var entry *TableEntry
	if needsTable {
		entry, err = s.materializeViaTable(cmd.Target, q, []string{activeSrc.Name}, src.Name, opStrs)
	} else {
		entry, err = s.materializeArrow(cmd.Target, q, []string{activeSrc.Name}, src.Name, opStrs)
	}
	if err != nil {
		return fmt.Errorf("pipe: %w", err)
	}
	fmt.Printf("→ %s: %d rows × %d cols\n", cmd.Target, entry.RowCount, entry.ColCount)
	return nil
}

// --- Builtin handlers ---

func execBuiltin(cmd *Cmd, s *Session) error {
	switch cmd.Builtin {
	case "zeaview":
		if len(cmd.Args) == 0 {
			return fmt.Errorf("zeaview: table name required")
		}
		return execZeaview(cmd.Args[0], s)
	case "hist":
		s.ShowHist()
		return nil
	case "status":
		s.ShowStatus()
		return nil
	case "drop":
		if len(cmd.Args) == 0 {
			return fmt.Errorf("drop: table name required (or use 'drop *' / 'drop --all' to clear session)")
		}
		if cmd.Args[0] == "*" || cmd.Args[0] == "--all" {
			names := make([]string, 0, len(s.Registry))
			for n := range s.Registry {
				names = append(names, n)
			}
			for _, n := range names {
				s.Drop(n)
			}
			fmt.Printf("dropped %d table(s)\n", len(names))
			return nil
		}
		if err := s.Drop(cmd.Args[0]); err != nil {
			return err
		}
		fmt.Printf("dropped %s\n", cmd.Args[0])
		return nil
	case "save":
		if len(cmd.Args) < 2 {
			return fmt.Errorf("save: usage: save <table> <path>")
		}
		return execSave(cmd.Args[0], cmd.Args[1], s)
	case "describe":
		if len(cmd.Args) == 0 {
			return fmt.Errorf("describe: table name required")
		}
		return execDescribe(cmd.Args[0], s)
	case "zearun":
		return execPluginRun(cmd.Args, s)
	case "zeaplugin":
		return execPluginManage(cmd.Args)
	case "zeadrive":
		return execZeadrive(cmd.Args, s)
	case "promote":
		return execPromote(cmd.Args, s)
	case "list":
		return execList(cmd.Args, s)
	case "validate":
		return execValidate(cmd.Args, s)
	case "export":
		return execExport(cmd.Args, s)
	case "publish":
		return execPublish(cmd.Args, s)
	case "push":
		return execPush(cmd.Args, s)
	case "iceberg":
		return execIceberg(cmd.Args, s)
	case "enable-s3":
		return s.Drive.execEnableS3()
	case "?", "help":
		execHelp()
		return nil
	}
	return fmt.Errorf("unknown builtin: %s", cmd.Builtin)
}

// execZeaview opens the zeashell TUI viewer for the named table.
// When Arrow records are in memory they are passed directly — zero copy,
// no IPC serialisation. Falls back to file-based viewing if records were evicted.
func execZeaview(name string, s *Session) error {
	entry, err := s.Get(name)
	if err != nil {
		return err
	}
	if entry.records == nil || entry.schema == nil {
		return tui.RunViewFromSource(entry.FilePath, nil)
	}
	return tui.RunViewFromArrow(entry.schema, entry.records, nil)
}

func execDescribe(name string, s *Session) error {
	entry, err := s.Get(name)
	if err != nil {
		return err
	}
	fmt.Printf("Table:   %s\n", entry.Name)
	fmt.Printf("Rows:    %d\n", entry.RowCount)
	fmt.Printf("Columns: %d\n", entry.ColCount)
	if entry.schema != nil {
		fmt.Printf("\n%-30s  %s\n", "Column", "Type")
		fmt.Println(strings.Repeat("─", 50))
		for i := 0; i < entry.schema.NumFields(); i++ {
			f := entry.schema.Field(i)
			fmt.Printf("%-30s  %s\n", f.Name, f.Type)
		}
	}
	if entry.Parent != "" || len(entry.Ops) > 0 {
		fmt.Println()
		if entry.Parent != "" {
			fmt.Printf("Parent:  %s\n", entry.Parent)
		}
		if len(entry.Ops) > 0 {
			fmt.Printf("Ops:     %s\n", strings.Join(entry.Ops, " | "))
		}
	}
	return nil
}

// execSave writes a session table to a file. Format is inferred from the
// extension (.parquet, .csv, .json, .jsonl). zea:// paths are expanded via
// DriveManager. The cloud mount is started automatically if needed.
func execSave(name, dest string, s *Session) error {
	entry, err := s.Get(name)
	if err != nil {
		return err
	}

	path := s.Drive.ExpandPath(dest)
	if strings.HasPrefix(path, s.Drive.MountPath) {
		if err := s.Drive.EnsureCloudMount(); err != nil {
			return err
		}
	}

	// Ensure parent directory exists.
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("save: %w", err)
	}

	ext := strings.ToLower(filepath.Ext(path))

	// If records were evicted to disk and not yet reloaded, fall back to
	// copying the spill file directly for Parquet output.
	if entry.records == nil && ext == ".parquet" && entry.FilePath != "" {
		data, err := os.ReadFile(entry.FilePath)
		if err != nil {
			return fmt.Errorf("save: %w", err)
		}
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return fmt.Errorf("save: %w", err)
		}
		fmt.Printf("saved %s → %s\n", name, dest)
		return nil
	}

	if entry.records == nil || entry.schema == nil {
		return fmt.Errorf("save: %s has no in-memory records", name)
	}

	if err := s.SaveTable(entry, path, ext); err != nil {
		return err
	}
	fmt.Printf("saved %s → %s\n", name, dest)
	return nil
}

// resolvePlugin finds a plugin by name, searching (in order):
//  1. ~/.zeaos/plugins/<name>      (ZeaOS-native executable)
//  2. ~/.zeaos/plugins/<name>.zea  (ZeaOS script)
//  3. ~/.zea/plugins/<name>        (zeashell-compatible)
//  4. ~/.zea/plugins/<name>.zea
func resolvePlugin(name string) (string, error) {
	home, _ := os.UserHomeDir()
	candidates := []string{
		filepath.Join(home, ".zeaos", "plugins", name),
		filepath.Join(home, ".zeaos", "plugins", name+".zea"),
		filepath.Join(home, ".zea", "plugins", name),
		filepath.Join(home, ".zea", "plugins", name+".zea"),
	}
	for _, p := range candidates {
		if info, err := os.Stat(p); err == nil && !info.IsDir() {
			return p, nil
		}
	}
	return "", fmt.Errorf("plugin %q not found — install it at ~/.zeaos/plugins/%s or ~/.zeaos/plugins/%s.zea", name, name, name)
}

// isZeaScript returns true if the file should be executed as a native ZeaOS
// script (lines fed through execLine in the current session) rather than as an
// external process. Criteria: .zea extension OR first line is "#!/usr/bin/env zeaos".
func isZeaScript(path string) bool {
	if strings.HasSuffix(path, ".zea") {
		return true
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	var line strings.Builder
	buf := make([]byte, 1)
	for {
		n, err := f.Read(buf)
		if n > 0 && buf[0] != '\n' {
			line.WriteByte(buf[0])
		}
		if err != nil || buf[0] == '\n' || line.Len() > 64 {
			break
		}
	}
	return strings.HasPrefix(line.String(), "#!/usr/bin/env zeaos")
}

// execZeaScript runs a .zea script file in the current session.
// Each non-blank, non-comment line is passed to execLine.
// The string $ARGS in any line is replaced with the space-joined plugin args,
// allowing scripts to forward flags like --repo to publish.
func execZeaScript(path string, args []string, s *Session) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("zearun: %w", err)
	}
	argsStr := strings.Join(args, " ")
	lineNum := 0
	for _, raw := range strings.Split(string(data), "\n") {
		lineNum++
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.ReplaceAll(line, "$ARGS", argsStr)
		if err := execLine(line, s); err != nil {
			return fmt.Errorf("zearun %s line %d: %w", filepath.Base(path), lineNum, err)
		}
	}
	return nil
}

// pluginEnv returns the environment for plugin execution: the current process
// environment plus ZeaOS-specific vars so scripts can locate session data.
func pluginEnv(s *Session) []string {
	env := os.Environ()
	if s != nil {
		env = append(env,
			"ZEAOS_SESSION_DIR="+s.Dir,
			"ZEAOS_TABLES_DIR="+s.TablesDir,
		)
	}
	return env
}

// extractZeaHelp returns the leading comment block from a .zea script,
// stripping the "# " prefix from each line. Stops at the first non-comment,
// non-blank line (i.e. the first actual command).
func extractZeaHelp(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	var out strings.Builder
	for i, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if i == 0 && strings.HasPrefix(line, "#!") {
			continue // skip shebang
		}
		if strings.HasPrefix(line, "#") {
			out.WriteString(strings.TrimPrefix(strings.TrimPrefix(line, "#"), " "))
			out.WriteByte('\n')
			continue
		}
		if line == "" {
			continue
		}
		break // first real command — stop
	}
	return strings.TrimSpace(out.String())
}

// showPluginHelp prints help for a plugin: extracts the comment header from
// .zea scripts, or runs the script with --help for executable scripts.
func showPluginHelp(scriptPath, name string) error {
	if isZeaScript(scriptPath) {
		help := extractZeaHelp(scriptPath)
		if help == "" {
			fmt.Printf("%s: no help text found (add # comments at the top of the script)\n", name)
		} else {
			fmt.Println(help)
		}
		return nil
	}
	c := exec.Command(scriptPath, "--help")
	c.Stdout, c.Stderr = os.Stdout, os.Stderr
	return c.Run()
}

// execPluginRun streams a plugin's output directly to the terminal.
// Syntax: zearun NAME [ARGS...]
func execPluginRun(args []string, s *Session) error {
	if len(args) == 0 {
		return fmt.Errorf("zearun: plugin name required")
	}
	scriptPath, err := resolvePlugin(args[0])
	if err != nil {
		return err
	}
	// zearun NAME --help
	if len(args) == 2 && args[1] == "--help" {
		return showPluginHelp(scriptPath, args[0])
	}
	if isZeaScript(scriptPath) {
		return execZeaScript(scriptPath, args[1:], s)
	}
	c := exec.Command(scriptPath, args[1:]...)
	c.Env = pluginEnv(s)
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
	return c.Run()
}

// execPluginCapture captures a plugin's CSV output as an Arrow table.
// Invoked via assignment: t = zearun NAME [ARGS...]
func execPluginCapture(cmd *Cmd, s *Session) error {
	if len(cmd.Args) == 0 {
		return fmt.Errorf("zearun: plugin name required")
	}
	pluginName := cmd.Args[0]
	scriptPath, err := resolvePlugin(pluginName)
	if err != nil {
		return err
	}
	if isZeaScript(scriptPath) {
		return fmt.Errorf("zearun %s: ZeaOS scripts run in the current session and don't produce tabular output — use 'zearun %s' without assignment", pluginName, pluginName)
	}

	var buf bytes.Buffer
	c := exec.Command(scriptPath, cmd.Args[1:]...)
	c.Env = pluginEnv(s)
	c.Stdout = &buf
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("zearun %s: %w", pluginName, err)
	}

	if buf.Len() == 0 {
		return fmt.Errorf("zearun %s: plugin produced no output — use 'zearun %s' (without assignment) for side-effect-only plugins", pluginName, pluginName)
	}

	tmp := filepath.Join(s.TablesDir, cmd.Target+"_plugin.csv")
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return err
	}
	defer os.Remove(tmp)

	q := fmt.Sprintf("SELECT * FROM read_csv_auto('%s')", sqlEsc(tmp))
	entry, err := s.materializeArrow(cmd.Target, q, nil, "",
		[]string{fmt.Sprintf("zearun(%s)", pluginName)})
	if err != nil {
		return fmt.Errorf("zearun %s: %w", pluginName, err)
	}
	fmt.Printf("→ %s: %d rows × %d cols\n", cmd.Target, entry.RowCount, entry.ColCount)
	return nil
}

// execPluginManage handles zeaplugin management subcommands.
// zeaplugin              — list plugins
// zeaplugin list         — list plugins
// zeaplugin <name> --help — show help for a specific plugin
func execPluginManage(args []string) error {
	if len(args) == 0 || args[0] == "list" {
		return execPluginList()
	}
	scriptPath, err := resolvePlugin(args[0])
	if err != nil {
		return err
	}
	return showPluginHelp(scriptPath, args[0])
}

// execPluginList prints plugins from both ZeaOS-native and zeashell directories.
func execPluginList() error {
	home, _ := os.UserHomeDir()
	dirs := []string{
		filepath.Join(home, ".zeaos", "plugins"),
		filepath.Join(home, ".zea", "plugins"),
	}

	type pluginEntry struct{ name, path string }
	seen := map[string]bool{}
	var plugins []pluginEntry

	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() && !seen[e.Name()] {
				seen[e.Name()] = true
				plugins = append(plugins, pluginEntry{e.Name(), filepath.Join(dir, e.Name())})
			}
		}
	}

	if len(plugins) == 0 {
		fmt.Printf("No plugins found. Install scripts at ~/.zeaos/plugins/ or ~/.zea/plugins/\n")
		return nil
	}
	fmt.Printf("%-24s  %s\n", "Plugin", "Path")
	fmt.Println(strings.Repeat("─", 60))
	for _, p := range plugins {
		fmt.Printf("%-24s  %s\n", p.name, p.path)
	}
	fmt.Println("\nRun 'zeaplugin <name> --help' for usage details.")
	return nil
}

func execHelp() {
	fmt.Print(`
ZeaOS — Command Reference

LOADING
  t = load <file>                    CSV / Parquet / JSON / TSV
  t = zeaql "SELECT ..."             SQL over session tables

TRANSFORMS  (chainable with |)
  t2 = t1 | where <expr>             filter rows     e.g. amount > 100
  t2 = t1 | select <cols>            pick columns    e.g. id, name, amount
  t2 = t1 | top <n>                  first N rows
  t2 = t1 | group <col>              count by column
  t2 = t1 | group <col> sum(<col2>)  aggregate
  t2 = t1 | pivot <col>→<val>        pivot table

SESSION
  t2 = t1                            alias / copy a table
  drop <table>                       remove table from session
  save <table> <path>                export to file  (.parquet / .csv / .json)
                                     path may be a zea:// URL
  hist                               table lineage DAG (TUI)
  status                             session status: tables, drive, memory (TUI)

VIEWER
  describe <table>                   show schema, row/col counts, lineage
  zeaview <table>                    open TUI viewer (s sort, f filter,
                                     g graph, e export, d schema, ? help)

DRIVE
  zea:// paths work everywhere — no mount required for local storage:
    t = load zea://data/file.parquet     local file in ~/.zeaos/local/data/
    ls zea://data/                       browse via shell commands

  Cloud backends (S3-compatible) require zeadrive:
    enable-s3                          configure S3 backend (opens TUI form)
    zeadrive mount                     mount cloud backends at ~/zeadrive
    zeadrive unmount                   unmount
    zeadrive status                    show local path, mount status, backends

    t = load zea://s3-data/file.parquet  cloud file via mounted backend

PROMOTE & EXPORT
  promote <table> [as <name>] [model|semantic]
                                     mark table for export promotion
  list                               list session tables
  list --type=promotions             list promoted artifacts
  validate <name> --target=dbt       check portability for target
  export [<name>] --target=dbt [-o DIR]
                                     write export bundle (default dir: ./zea-dbt-export)

PUSH
  push --target md:database          push session tables to MotherDuck
  push --target zea://backend/path   push to ZeaDrive as flat Parquet
  push --target zea://... --iceberg  push as Apache Iceberg v2 table (ZeaDrive)
  push status                        show push history
  push sync --target md:database     check for drift and re-push if stale

ICEBERG
  iceberg verify [<table>...]        verify snapshot SHA-256 hashes against remote
                                     accepts table name or zea:// path
                                     tracks change history across verify runs
  iceberg repair <table>...          re-copy metadata to remote after a failed push

PLUGINS
  zearun <name> [args]               run plugin, stream output to terminal
  t = zearun <name> [args]           run plugin, capture CSV output as table

  zeaplugin                          list all available plugins
  zeaplugin list                     list all available plugins
  zeaplugin <name> --help            show help for a specific plugin

OTHER
  exit / quit                        exit ZeaOS
  ?  or  help                        show this help

`)
}

// execZeadrive dispatches zeadrive subcommands via the session's DriveManager.
func execZeadrive(args []string, s *Session) error {
	return s.Drive.Exec(args)
}

// execOSPipe falls back to the system shell for unrecognised input.
// Any zea:// tokens are expanded via DriveManager before passing to the shell.
func execOSPipe(line string, s *Session) error {
	if strings.Contains(line, "zea://") {
		// Replace each zea:// token individually so backend vs local routing applies.
		parts := strings.Fields(line)
		for i, p := range parts {
			if strings.Contains(p, "zea://") {
				parts[i] = s.Drive.ExpandPath(p)
			}
		}
		line = strings.Join(parts, " ")
	}
	c := exec.Command("sh", "-c", line)
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
	return c.Run()
}

// inferSQLParent returns the first session table name found as a whole word in
// the SQL string. Used to set the Parent field on SQL-derived tables so the
// lineage tree reflects the actual derivation chain.
// inferSQLParent returns the first session table name found as a whole word in
// the SQL string. Used to set the Parent field on SQL-derived tables so the
// lineage tree reflects the actual derivation chain.
func inferSQLParent(sql string, tableNames []string) string {
	upper := strings.ToUpper(sql)
	for _, name := range tableNames {
		upperName := strings.ToUpper(name)
		search := upper
		for {
			idx := strings.Index(search, upperName)
			if idx < 0 {
				break
			}
			absIdx := len(upper) - len(search) + idx
			before := absIdx == 0 || !isIdentChar(rune(upper[absIdx-1]))
			end := absIdx + len(name)
			after := end >= len(upper) || !isIdentChar(rune(upper[end]))
			if before && after {
				return name
			}
			search = search[idx+1:]
		}
	}
	return ""
}

// sqlOps returns a descriptive Ops slice for a SQL-derived table. Detects
// iceberg_scan so the lineage label is more informative than just "sql".
func sqlOps(sql string) []string {
	upper := strings.ToUpper(sql)
	if idx := strings.Index(upper, "ICEBERG_SCAN("); idx >= 0 {
		// Extract the path argument from iceberg_scan('...').
		rest := sql[idx+len("iceberg_scan("):]
		end := strings.IndexAny(rest, ")'\"")
		if end > 0 {
			path := strings.Trim(rest[:end], `'"`)
			// Use just the last two path components to keep the label short.
			parts := strings.Split(strings.TrimRight(path, "/"), "/")
			short := path
			if len(parts) >= 2 {
				short = strings.Join(parts[len(parts)-2:], "/")
			}
			return []string{"iceberg_scan(" + short + ")"}
		}
	}
	return []string{"sql"}
}

func isIdentChar(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9') || r == '_'
}
