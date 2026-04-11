package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

type CmdType int

const (
	CmdAssignment CmdType = iota
	CmdBuiltin
	CmdOSPipe
)

// PipeOp represents one segment of a shorthand pipe chain.
type PipeOp struct {
	Kind string // where | pivot | top | select | group
	Args string
}

// Cmd is the parsed representation of a single REPL input line.
type Cmd struct {
	Type    CmdType
	Target  string   // LHS of assignment (t1)
	Source  string   // RHS source: table name, "load", or "sql"
	File    string   // resolved file path for load
	RawSQL  string   // query string for zeaql "..."
	Ops     []PipeOp // pipe operations after the source table
	Builtin string   // name of builtin command
	Args    []string // args to builtin or OS pipe
	Raw     string   // original input line
}

// ParseLine turns a REPL input line into a Cmd.
func ParseLine(line string) (*Cmd, error) {
	cmd := &Cmd{Raw: line}

	// Assignment: IDENT = RHS
	if eq := strings.Index(line, "="); eq > 0 {
		lhs := strings.TrimSpace(line[:eq])
		if isIdent(lhs) {
			cmd.Type = CmdAssignment
			cmd.Target = lhs
			return parseRHS(cmd, strings.TrimSpace(line[eq+1:]))
		}
	}

	// Builtins
	parts := shellSplit(line)
	if len(parts) > 0 {
		switch parts[0] {
		case "zeaview", "hist", "status", "drop", "save", "model", "list", "push", "iceberg", "zearun", "zeaplugin", "zeadrive", "describe", "version", "?", "help", "enable-s3":
			cmd.Type = CmdBuiltin
			cmd.Builtin = parts[0]
			cmd.Args = parts[1:]
			return cmd, nil
		}
	}

	// Anything else falls through to the OS shell
	cmd.Type = CmdOSPipe
	return cmd, nil
}

func parseRHS(cmd *Cmd, rhs string) (*Cmd, error) {
	// load FILE
	if rest, ok := cutPrefix(rhs, "load "); ok {
		cmd.Source = "load"
		cmd.File = expandHome(strings.TrimSpace(rest))
		return cmd, nil
	}

	// zeaql "QUERY"
	if rest, ok := cutPrefix(rhs, "zeaql "); ok {
		q := strings.Trim(strings.TrimSpace(rest), `"'`)
		cmd.Source = "sql"
		cmd.RawSQL = q
		return cmd, nil
	}

	// zearun NAME [ARGS...] — capture plugin output as table
	if rest, ok := cutPrefix(rhs, "zearun "); ok {
		cmd.Source = "zearun"
		cmd.Args = shellSplit(strings.TrimSpace(rest))
		return cmd, nil
	}

	// TABLE | op | op ...
	if pipe := strings.Index(rhs, " | "); pipe >= 0 {
		cmd.Source = strings.TrimSpace(rhs[:pipe])
		ops, err := parsePipes(strings.TrimSpace(rhs[pipe+3:]))
		if err != nil {
			return nil, err
		}
		cmd.Ops = ops
		return cmd, nil
	}

	// Simple alias: t2 = t1
	if isIdent(strings.TrimSpace(rhs)) {
		cmd.Source = strings.TrimSpace(rhs)
		return cmd, nil
	}

	return nil, fmt.Errorf("cannot parse expression: %q", rhs)
}

func parsePipes(s string) ([]PipeOp, error) {
	segments := strings.Split(s, " | ")
	ops := make([]PipeOp, 0, len(segments))
	for _, seg := range segments {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		op, err := parseSingleOp(seg)
		if err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	return ops, nil
}

func parseSingleOp(seg string) (PipeOp, error) {
	kv := strings.SplitN(seg, " ", 2)
	kind := strings.ToLower(kv[0])
	args := ""
	if len(kv) > 1 {
		args = strings.TrimSpace(kv[1])
	}
	switch kind {
	case "where", "top", "select", "group", "pivot":
		return PipeOp{Kind: kind, Args: args}, nil
	default:
		return PipeOp{}, fmt.Errorf("unknown pipe op %q — valid ops: where, top, select, group, pivot", kind)
	}
}

// BuildSQL translates a source Parquet path + pipe ops into a DuckDB SELECT.
// Each op wraps the previous query as a subquery.
func BuildSQL(srcPath string, ops []PipeOp) (string, error) {
	q := fmt.Sprintf("SELECT * FROM read_parquet('%s')", sqlEsc(srcPath))
	for _, op := range ops {
		var err error
		q, err = applyOp(q, op)
		if err != nil {
			return "", err
		}
	}
	return q, nil
}

// BuildSQLFromView translates a registered Arrow view name + pipe ops into a
// DuckDB SELECT. Used for in-memory tables where the source is a DuckDB VIEW
// over an Arrow scan rather than a Parquet file on disk.
func BuildSQLFromView(viewName string, ops []PipeOp) (string, error) {
	q := fmt.Sprintf(`SELECT * FROM "%s"`, viewName)
	for _, op := range ops {
		var err error
		q, err = applyOp(q, op)
		if err != nil {
			return "", err
		}
	}
	return q, nil
}

func applyOp(inner string, op PipeOp) (string, error) {
	switch op.Kind {
	case "where":
		return fmt.Sprintf("SELECT * FROM (%s) _z WHERE %s", inner, op.Args), nil
	case "top":
		return fmt.Sprintf("SELECT * FROM (%s) _z LIMIT %s", inner, op.Args), nil
	case "select":
		return fmt.Sprintf("SELECT %s FROM (%s) _z", normCols(op.Args), inner), nil
	case "group":
		return buildGroup(inner, op.Args)
	case "pivot":
		return buildPivot(inner, op.Args)
	}
	return "", fmt.Errorf("unknown op: %s", op.Kind)
}

// group COL [fn(COL)]
// e.g. "group species" → SELECT species, COUNT(*) AS _count FROM ... GROUP BY species
// e.g. "group species sum(speed)" → SELECT species, SUM(speed) AS sum_speed FROM ... GROUP BY species
func buildGroup(inner, args string) (string, error) {
	parts := strings.Fields(args)
	if len(parts) == 0 {
		return "", fmt.Errorf("group: column name required")
	}
	groupCol := parts[0]
	agg := "COUNT(*) AS _count"
	if len(parts) > 1 {
		raw := parts[1]
		if lp := strings.Index(raw, "("); lp > 0 {
			fn := strings.ToUpper(raw[:lp])
			col := strings.Trim(raw[lp:], "()")
			agg = fmt.Sprintf("%s(%s) AS %s_%s", fn, col, strings.ToLower(fn), col)
		}
	}
	return fmt.Sprintf("SELECT %s, %s FROM (%s) _z GROUP BY %s",
		groupCol, agg, inner, groupCol), nil
}

// pivot COL→VALUE_COL  (also accepts ->)
// e.g. "pivot status→amount" → PIVOT ... ON status USING first(amount)
func buildPivot(inner, args string) (string, error) {
	args = strings.ReplaceAll(args, "→", "->")
	parts := strings.SplitN(args, "->", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("pivot: expected COL→VALUE_COL (e.g. status→amount)")
	}
	pivotCol := strings.TrimSpace(parts[0])
	valCol := strings.TrimSpace(parts[1])
	return fmt.Sprintf("PIVOT (%s) _z ON %s USING first(%s)", inner, pivotCol, valCol), nil
}

func normCols(s string) string {
	parts := strings.Split(s, ",")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}
	return strings.Join(parts, ", ")
}

func isIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if i == 0 && !unicode.IsLetter(r) && r != '_' {
			return false
		}
		if i > 0 && !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}
	return true
}

// shellSplit splits on whitespace while respecting single and double quotes.
func shellSplit(s string) []string {
	var parts []string
	var cur strings.Builder
	inQ, qc := false, rune(0)
	for _, r := range s {
		switch {
		case inQ && r == qc:
			inQ = false
		case inQ:
			cur.WriteRune(r)
		case r == '\'' || r == '"':
			inQ, qc = true, r
		case r == ' ' || r == '\t':
			if cur.Len() > 0 {
				parts = append(parts, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		parts = append(parts, cur.String())
	}
	return parts
}

func cutPrefix(s, prefix string) (string, bool) {
	if strings.HasPrefix(s, prefix) {
		return s[len(prefix):], true
	}
	return "", false
}

func expandHome(path string) string {
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, path[2:])
		}
	}
	return path
}

