package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

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
	file := s.Drive.ExpandPath(cmd.File)
	// If the path routed to the FUSE mount, ensure Volumez is running.
	if strings.HasPrefix(file, s.Drive.MountPath) {
		if err := s.Drive.EnsureCloudMount(); err != nil {
			return err
		}
	}
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
		[]string{fmt.Sprintf("load(%s)", filepath.Base(file))})
	if err != nil {
		return fmt.Errorf("load %s: %w", file, err)
	}
	fmt.Printf("→ %s: %d rows × %d cols\n", cmd.Target, entry.RowCount, entry.ColCount)
	return nil
}

func execSQL(cmd *Cmd, s *Session) error {
	// Register all session tables so the user's SQL can reference any of them.
	srcs := make([]string, 0, len(s.Registry))
	for name := range s.Registry {
		srcs = append(srcs, name)
	}
	entry, err := s.materializeArrow(cmd.Target, cmd.RawSQL, srcs, "", []string{"sql"})
	if err != nil {
		return fmt.Errorf("sql: %w", err)
	}
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
	case "describe":
		if len(cmd.Args) == 0 {
			return fmt.Errorf("describe: table name required")
		}
		return execDescribe(cmd.Args[0], s)
	case "zeaplugin":
		return execPlugin(cmd.Args, s)
	case "zeadrive":
		return execZeadrive(cmd.Args, s)
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

// execPlugin runs a zeashell plugin via `zea run`.
// Syntax: zeaplugin NAME [ARGS...] [→ TARGET] or zeaplugin NAME→TARGET
func execPlugin(args []string, s *Session) error {
	if len(args) == 0 {
		return fmt.Errorf("zeaplugin: plugin name required")
	}

	pluginName := args[0]
	pluginArgs := args[1:]
	var target string

	// zeaplugin name → target  (spaced arrow)
	for i, a := range pluginArgs {
		if (a == "→" || a == "->") && i+1 < len(pluginArgs) {
			target = pluginArgs[i+1]
			pluginArgs = pluginArgs[:i]
			break
		}
	}
	// zeaplugin name→target  (no spaces)
	if target == "" {
		for _, sep := range []string{"→", "->"} {
			if idx := strings.Index(pluginName, sep); idx > 0 {
				target = pluginName[idx+len(sep):]
				pluginName = pluginName[:idx]
				break
			}
		}
	}

	zeaBin, err := exec.LookPath("zea")
	if err != nil {
		return fmt.Errorf("zea binary not found in PATH — is zeashell built and installed?")
	}

	runArgs := append([]string{"run", pluginName}, pluginArgs...)

	if target == "" {
		// No capture: stream output directly to terminal
		c := exec.Command(zeaBin, runArgs...)
		c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
		return c.Run()
	}

	// Capture CSV output → Arrow table (via materializeArrow with CSV read)
	var buf bytes.Buffer
	c := exec.Command(zeaBin, runArgs...)
	c.Stdout = &buf
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("plugin %s: %w", pluginName, err)
	}

	tmp := filepath.Join(s.TablesDir, target+"_plugin.csv")
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		return err
	}
	defer os.Remove(tmp)

	q := fmt.Sprintf("SELECT * FROM read_csv_auto('%s')", sqlEsc(tmp))
	entry, err := s.materializeArrow(target, q, nil, "",
		[]string{fmt.Sprintf("plugin(%s)", pluginName)})
	if err != nil {
		return fmt.Errorf("plugin output: %w", err)
	}
	fmt.Printf("→ %s: %d rows × %d cols\n", target, entry.RowCount, entry.ColCount)
	return nil
}

func execHelp() {
	fmt.Print(`
ZeaOS — Command Reference

LOADING
  t = load <file>                    CSV / Parquet / JSON / TSV
  t = zea sql "SELECT ..."           SQL over session tables

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

PLUGINS
  zeaplugin <name> [args]            run plugin, stream output
  zeaplugin <name> [args] → <table>  run plugin, capture as table

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
