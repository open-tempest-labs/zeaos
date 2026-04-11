package main

import (
	"testing"
)

func TestParseLine_assignment_zeaql(t *testing.T) {
	cmd, err := ParseLine(`t = zeaql "SELECT * FROM trips"`)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Type != CmdAssignment {
		t.Errorf("type: got %v want CmdAssignment", cmd.Type)
	}
	if cmd.Target != "t" {
		t.Errorf("target: got %q want %q", cmd.Target, "t")
	}
	if cmd.RawSQL != "SELECT * FROM trips" {
		t.Errorf("sql: got %q", cmd.RawSQL)
	}
}

func TestParseLine_assignment_load(t *testing.T) {
	cmd, err := ParseLine("trips = load data.parquet")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Source != "load" {
		t.Errorf("source: got %q want load", cmd.Source)
	}
	if cmd.Target != "trips" {
		t.Errorf("target: got %q", cmd.Target)
	}
}

func TestParseLine_assignment_alias(t *testing.T) {
	cmd, err := ParseLine("t2 = t1")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Type != CmdAssignment {
		t.Errorf("type: got %v", cmd.Type)
	}
	if cmd.Source != "t1" {
		t.Errorf("source: got %q", cmd.Source)
	}
}

func TestParseLine_pipe(t *testing.T) {
	cmd, err := ParseLine("out = trips | where trip_distance > 2.0 | top 100")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Source != "trips" {
		t.Errorf("source: got %q", cmd.Source)
	}
	if len(cmd.Ops) != 2 {
		t.Fatalf("ops: got %d want 2", len(cmd.Ops))
	}
	if cmd.Ops[0].Kind != "where" || cmd.Ops[0].Args != "trip_distance > 2.0" {
		t.Errorf("op[0]: %+v", cmd.Ops[0])
	}
	if cmd.Ops[1].Kind != "top" || cmd.Ops[1].Args != "100" {
		t.Errorf("op[1]: %+v", cmd.Ops[1])
	}
}

func TestParseLine_builtin(t *testing.T) {
	for _, line := range []string{"list", "push status", "iceberg verify foo", "hist", "model list", "model promote foo"} {
		cmd, err := ParseLine(line)
		if err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		if cmd.Type != CmdBuiltin {
			t.Errorf("%q: type got %v want CmdBuiltin", line, cmd.Type)
		}
	}
}

func TestParseLine_os_pipe(t *testing.T) {
	cmd, err := ParseLine("ls -la")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Type != CmdOSPipe {
		t.Errorf("type: got %v want CmdOSPipe", cmd.Type)
	}
}

func TestBuildSQL_where(t *testing.T) {
	sql, err := BuildSQL("/tmp/data.parquet", []PipeOp{
		{Kind: "where", Args: "x > 1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT * FROM (SELECT * FROM read_parquet('/tmp/data.parquet')) _z WHERE x > 1`
	if sql != want {
		t.Errorf("got  %q\nwant %q", sql, want)
	}
}

func TestBuildSQL_top(t *testing.T) {
	sql, err := BuildSQL("/tmp/data.parquet", []PipeOp{
		{Kind: "top", Args: "5"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT * FROM (SELECT * FROM read_parquet('/tmp/data.parquet')) _z LIMIT 5`
	if sql != want {
		t.Errorf("got %q", sql)
	}
}

func TestBuildSQL_group(t *testing.T) {
	sql, err := BuildSQL("/tmp/data.parquet", []PipeOp{
		{Kind: "group", Args: "status"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `SELECT status, COUNT(*) AS _count FROM (SELECT * FROM read_parquet('/tmp/data.parquet')) _z GROUP BY status`
	if sql != want {
		t.Errorf("got %q", sql)
	}
}

func TestBuildSQL_unknown_op(t *testing.T) {
	_, err := BuildSQL("/tmp/data.parquet", []PipeOp{
		{Kind: "explode", Args: "col"},
	})
	if err == nil {
		t.Error("expected error for unknown op")
	}
}

func TestShellSplit(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{`push avg_tip --target md:my_db`, []string{"push", "avg_tip", "--target", "md:my_db"}},
		{`iceberg verify "my table"`, []string{"iceberg", "verify", "my table"}},
		{`load 'file with spaces.parquet'`, []string{"load", "file with spaces.parquet"}},
	}
	for _, c := range cases {
		got := shellSplit(c.in)
		if len(got) != len(c.want) {
			t.Errorf("shellSplit(%q): got %v want %v", c.in, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("shellSplit(%q)[%d]: got %q want %q", c.in, i, got[i], c.want[i])
			}
		}
	}
}
