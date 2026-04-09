package zeaberg_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/open-tempest-labs/zeaberg-go"
)

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()

	// Build a small Arrow schema and write a Parquet file.
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	parquetPath := filepath.Join(dir, "data.parquet")
	rowCount := int64(writeTestParquet(t, parquetPath, arrowSchema))

	// Create the Iceberg table.
	tbl, err := zeaberg.CreateTable(filepath.Join(dir, "table"), arrowSchema)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Append a snapshot with lineage metadata.
	lineage := &zeaberg.LineageInfo{
		SessionID:  "test-session-001",
		SourceURIs: []string{"https://example.com/data.parquet"},
		PromotedAs: "test_table",
		ExportedAt: time.Now().UTC(),
		Chain: []zeaberg.ChainEntry{
			{Name: "raw", Operation: "load", SourceURI: "https://example.com/data.parquet"},
			{Name: "test_table", Operation: "sql", Parent: "raw"},
		},
	}
	err = tbl.AppendSnapshot(parquetPath, rowCount,
		zeaberg.WithLineage(lineage),
	)
	if err != nil {
		t.Fatalf("AppendSnapshot: %v", err)
	}

	// Verify snapshot was recorded.
	if tbl.SnapshotCount() != 1 {
		t.Errorf("expected 1 snapshot, got %d", tbl.SnapshotCount())
	}
	snapID, err := tbl.CurrentSnapshotID()
	if err != nil {
		t.Fatalf("CurrentSnapshotID: %v", err)
	}
	if snapID == 0 {
		t.Error("snapshot ID is zero")
	}

	// Verify metadata files exist on disk.
	tableDir := filepath.Join(dir, "table")
	checkFile(t, tableDir, "metadata/version-hint.text")
	checkFile(t, tableDir, "metadata/v1.metadata.json")
	checkFile(t, tableDir, "metadata/v2.metadata.json")

	// Verify manifest list avro exists.
	metaDir := filepath.Join(tableDir, "metadata")
	entries, err := os.ReadDir(metaDir)
	if err != nil {
		t.Fatalf("ReadDir metadata: %v", err)
	}
	var avroFiles []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".avro" {
			avroFiles = append(avroFiles, e.Name())
		}
	}
	if len(avroFiles) < 2 {
		t.Errorf("expected at least 2 avro files (manifest + manifest list), got %d: %v", len(avroFiles), avroFiles)
	}

	// Append a second snapshot to verify the log grows.
	err = tbl.AppendSnapshot(parquetPath, rowCount,
		zeaberg.WithSessionID("test-session-002"),
		zeaberg.WithSourceURIs("https://example.com/data-v2.parquet"),
	)
	if err != nil {
		t.Fatalf("AppendSnapshot (2nd): %v", err)
	}
	if tbl.SnapshotCount() != 2 {
		t.Errorf("expected 2 snapshots after second append, got %d", tbl.SnapshotCount())
	}

	// Open the table fresh and verify it round-trips.
	tbl2, err := zeaberg.OpenTable(tableDir)
	if err != nil {
		t.Fatalf("OpenTable: %v", err)
	}
	if tbl2.SnapshotCount() != 2 {
		t.Errorf("reopened table: expected 2 snapshots, got %d", tbl2.SnapshotCount())
	}
}

func checkFile(t *testing.T, base, rel string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(base, rel)); err != nil {
		t.Errorf("expected file %s: %v", rel, err)
	}
}

// writeTestParquet writes a small Parquet file with 3 rows and returns the row count.
func writeTestParquet(t *testing.T, path string, schema *arrow.Schema) int {
	t.Helper()
	mem := memory.NewGoAllocator()

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"alpha", "beta", "gamma"}, nil)
	b.Field(2).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)

	rec := b.NewRecord()
	defer rec.Release()

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create parquet: %v", err)
	}
	defer f.Close()

	w, err := pqarrow.NewFileWriter(schema, f, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("parquet writer: %v", err)
	}
	if err := w.Write(rec); err != nil {
		t.Fatalf("parquet write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("parquet close: %v", err)
	}
	return int(rec.NumRows())
}
