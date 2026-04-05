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

	var entry *TableEntry
	var err error
	if needsTable {
		entry, err = s.materializeViaTable(cmd.Target, cmd.RawSQL, srcs, "", []string{"sql"})
	} else {
		entry, err = s.materializeArrow(cmd.Target, cmd.RawSQL, srcs, "", []string{"sql"})
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
			return fmt.Errorf("drop: table name required")
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
	case "dbt":
		return execDbt(cmd.Args, s)
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

// execPluginRun streams a plugin's output directly to the terminal.
// Syntax: zearun NAME [ARGS...]
func execPluginRun(args []string, s *Session) error {
	if len(args) == 0 {
		return fmt.Errorf("zearun: plugin name required")
	}
	zeaBin, err := exec.LookPath("zea")
	if err != nil {
		return fmt.Errorf("zea binary not found in PATH — is zeashell installed?")
	}
	c := exec.Command(zeaBin, append([]string{"run", args[0]}, args[1:]...)...)
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
	pluginArgs := cmd.Args[1:]

	zeaBin, err := exec.LookPath("zea")
	if err != nil {
		return fmt.Errorf("zea binary not found in PATH — is zeashell installed?")
	}

	var buf bytes.Buffer
	c := exec.Command(zeaBin, append([]string{"run", pluginName}, pluginArgs...)...)
	c.Stdout = &buf
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("zearun %s: %w", pluginName, err)
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
	// zeaplugin <name> --help
	name := args[0]
	zeaBin, err := exec.LookPath("zea")
	if err != nil {
		return fmt.Errorf("zea binary not found in PATH — is zeashell installed?")
	}
	c := exec.Command(zeaBin, "run", name, "--help")
	c.Stdout, c.Stderr = os.Stdout, os.Stderr
	return c.Run()
}

// execPluginList prints all available zeashell plugins from ~/.zea/plugins/.
func execPluginList() error {
	home, _ := os.UserHomeDir()
	pluginDir := filepath.Join(home, ".zea", "plugins")
	entries, err := os.ReadDir(pluginDir)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("No plugins found. Plugin directory does not exist: ~/.zea/plugins/")
			return nil
		}
		return fmt.Errorf("zeaplugin list: %w", err)
	}
	if len(entries) == 0 {
		fmt.Println("No plugins installed in ~/.zea/plugins/")
		return nil
	}
	fmt.Printf("%-24s  %s\n", "Plugin", "Path")
	fmt.Println(strings.Repeat("─", 60))
	for _, e := range entries {
		if !e.IsDir() {
			fmt.Printf("%-24s  %s\n", e.Name(),
				filepath.Join(pluginDir, e.Name()))
		}
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

DBT EXPORT
  dbt promote <table> [as <name>] [model|semantic]
                                     mark table for dbt export
  dbt list                           list promoted artifacts
  dbt validate [<name>]              check portability without writing files
  dbt export [<name>] [--target DIR] write dbt Core project files
  dbt <subcommand>                   any other subcommand passed to dbt CLI

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
