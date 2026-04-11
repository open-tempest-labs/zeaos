package zeaberg_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/open-tempest-labs/zeaberg-go"
)

// writeComplianceParquet writes a minimal Parquet file matching the given schema.
// All Int64 columns get values [1,2,3]; all Float64 columns get [1.1,2.2,3.3].
func writeComplianceParquet(t *testing.T, path string, schema *arrow.Schema) {
	t.Helper()
	mem := memory.NewGoAllocator()
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	for i := 0; i < schema.NumFields(); i++ {
		switch schema.Field(i).Type {
		case arrow.PrimitiveTypes.Int64:
			b.Field(i).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
		case arrow.PrimitiveTypes.Float64:
			b.Field(i).(*array.Float64Builder).AppendValues([]float64{1.1, 2.2, 3.3}, nil)
		default:
			t.Fatalf("writeComplianceParquet: unsupported field type %v", schema.Field(i).Type)
		}
	}
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
	w.Close()
}

// TestVersionHintBareInteger verifies that version-hint.text contains only the
// bare integer with no padding or newline. DuckDB constructs the metadata
// filename from the raw file content and fails if extra whitespace is present.
func TestVersionHintBareInteger(t *testing.T) {
	dir := t.TempDir()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	parquetPath := filepath.Join(dir, "data.parquet")
	writeComplianceParquet(t, parquetPath, schema)

	tbl, err := zeaberg.CreateTable(filepath.Join(dir, "table"), schema)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := tbl.AppendSnapshot(parquetPath, 1); err != nil {
		t.Fatalf("AppendSnapshot: %v", err)
	}

	hintPath := filepath.Join(dir, "table", "metadata", "version-hint.text")
	data, err := os.ReadFile(hintPath)
	if err != nil {
		t.Fatalf("read version-hint.text: %v", err)
	}
	content := string(data)

	if strings.TrimSpace(content) != content {
		t.Errorf("version-hint.text has surrounding whitespace: %q", content)
	}
	if _, err := strconv.Atoi(content); err != nil {
		t.Errorf("version-hint.text is not a bare integer: %q", content)
	}
}

// TestSequenceNumberStartsAtOne verifies that the first snapshot's
// last-sequence-number in the table metadata is ≥ 1. DuckDB rejects 0.
func TestSequenceNumberStartsAtOne(t *testing.T) {
	dir := t.TempDir()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	parquetPath := filepath.Join(dir, "data.parquet")
	writeComplianceParquet(t, parquetPath, schema)

	tbl, err := zeaberg.CreateTable(filepath.Join(dir, "table"), schema)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := tbl.AppendSnapshot(parquetPath, 1); err != nil {
		t.Fatalf("AppendSnapshot: %v", err)
	}

	metaPath := filepath.Join(dir, "table", "metadata", "v2.metadata.json")
	data, err := os.ReadFile(metaPath)
	if err != nil {
		t.Fatalf("read metadata: %v", err)
	}

	var meta struct {
		LastSequenceNumber int64 `json:"last-sequence-number"`
	}
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatalf("parse metadata: %v", err)
	}
	if meta.LastSequenceNumber < 1 {
		t.Errorf("last-sequence-number = %d, want ≥ 1", meta.LastSequenceNumber)
	}
}

// TestRewriteWithFieldIDs verifies that RewriteWithFieldIDs embeds 1-based
// PARQUET:field_id metadata on every column. DuckDB iceberg_scan uses field
// IDs to match columns; without them every column reads as NULL.
func TestRewriteWithFieldIDs(t *testing.T) {
	dir := t.TempDir()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "payment_type", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "avg_tip", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "trips", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	src := filepath.Join(dir, "src.parquet")
	writeComplianceParquet(t, src, schema)

	dst := filepath.Join(dir, "dst.parquet")
	if err := zeaberg.RewriteWithFieldIDs(src, dst); err != nil {
		t.Fatalf("RewriteWithFieldIDs: %v", err)
	}

	// Read back the rewritten file and check field IDs.
	f, err := os.Open(dst)
	if err != nil {
		t.Fatalf("open rewritten parquet: %v", err)
	}
	defer f.Close()
	pf, err := file.NewParquetReader(f)
	if err != nil {
		t.Fatalf("parquet reader: %v", err)
	}
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("arrow file reader: %v", err)
	}
	arrowSchema, err := fr.Schema()
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	// Consume the reader to avoid resource leak.
	_, _ = fr.ReadTable(context.Background())

	for i := 0; i < arrowSchema.NumFields(); i++ {
		f := arrowSchema.Field(i)
		if f.Metadata.Len() == 0 {
			t.Errorf("field %q has no metadata", f.Name)
			continue
		}
		idx := f.Metadata.FindKey("PARQUET:field_id")
		if idx < 0 {
			t.Errorf("field %q missing PARQUET:field_id", f.Name)
			continue
		}
		got, err := strconv.Atoi(f.Metadata.Values()[idx])
		if err != nil {
			t.Errorf("field %q PARQUET:field_id not an int: %q", f.Name, f.Metadata.Values()[idx])
			continue
		}
		want := i + 1
		if got != want {
			t.Errorf("field %q field_id = %d, want %d", f.Name, got, want)
		}
	}
}

// TestVerifyTableDetectsCorruption verifies that VerifyTable returns a mismatch
// status when the data file content has changed after the snapshot was created.
func TestVerifyTableDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	parquetPath := filepath.Join(dir, "data.parquet")
	writeComplianceParquet(t, parquetPath, schema)

	tbl, err := zeaberg.CreateTable(filepath.Join(dir, "table"), schema)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := tbl.AppendSnapshot(parquetPath, 1); err != nil {
		t.Fatalf("AppendSnapshot: %v", err)
	}

	// AppendSnapshot copies the file into table/data/; corrupt that copy.
	matches, err := filepath.Glob(filepath.Join(dir, "table", "data", "*.parquet"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("finding data file: err=%v matches=%v", err, matches)
	}
	if err := os.WriteFile(matches[0], []byte("corrupted"), 0644); err != nil {
		t.Fatalf("corrupt: %v", err)
	}

	_, results, err := zeaberg.VerifyTable(filepath.Join(dir, "table"))
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Status != zeaberg.VerifyStatusMismatch {
		t.Errorf("status = %v, want Mismatch", results[0].Status)
	}
}

// TestVerifyTableOK verifies that VerifyTable returns OK for an untouched file.
func TestVerifyTableOK(t *testing.T) {
	dir := t.TempDir()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)

	parquetPath := filepath.Join(dir, "data.parquet")
	writeComplianceParquet(t, parquetPath, schema)

	tbl, err := zeaberg.CreateTable(filepath.Join(dir, "table"), schema)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}
	if err := tbl.AppendSnapshot(parquetPath, 1); err != nil {
		t.Fatalf("AppendSnapshot: %v", err)
	}

	_, results, err := zeaberg.VerifyTable(filepath.Join(dir, "table"))
	if err != nil {
		t.Fatalf("VerifyTable: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Status != zeaberg.VerifyStatusOK {
		t.Errorf("status = %v, want OK", results[0].Status)
	}
}
