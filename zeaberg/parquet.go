package zeaberg

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// RewriteWithFieldIDs reads srcPath, stamps each column with its Iceberg field
// ID (1-based, matching SchemaFromArrow order), and writes the result to
// dstPath. DuckDB's iceberg_scan uses field IDs to match Parquet columns to
// the Iceberg schema; without them every column reads as NULL.
func RewriteWithFieldIDs(srcPath, dstPath string) error {
	in, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open parquet: %w", err)
	}
	defer in.Close()

	pf, err := file.NewParquetReader(in)
	if err != nil {
		return fmt.Errorf("open parquet reader: %w", err)
	}

	mem := memory.DefaultAllocator
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return fmt.Errorf("arrow file reader: %w", err)
	}

	tbl, err := fr.ReadTable(context.Background())
	if err != nil {
		return fmt.Errorf("read parquet table: %w", err)
	}
	defer tbl.Release()

	// Rebuild schema with PARQUET:field_id stamped on each field (1-based).
	oldSchema := tbl.Schema()
	newFields := make([]arrow.Field, oldSchema.NumFields())
	for i := 0; i < oldSchema.NumFields(); i++ {
		f := oldSchema.Field(i)
		newFields[i] = arrow.Field{
			Name:     f.Name,
			Type:     f.Type,
			Nullable: f.Nullable,
			Metadata: arrow.NewMetadata(
				[]string{"PARQUET:field_id"},
				[]string{strconv.Itoa(i + 1)},
			),
		}
	}
	schemaMeta := oldSchema.Metadata()
	newSchema := arrow.NewSchema(newFields, &schemaMeta)

	// Reconstruct table with the new schema. Columns are unchanged.
	cols := make([]arrow.Column, tbl.NumCols())
	for i := 0; i < int(tbl.NumCols()); i++ {
		cols[i] = *tbl.Column(i)
	}
	newTbl := array.NewTable(newSchema, cols, tbl.NumRows())
	defer newTbl.Release()

	out, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("create output: %w", err)
	}
	defer out.Close()

	wrProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	arrProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())

	if err := pqarrow.WriteTable(newTbl, out, 1024*1024, wrProps, arrProps); err != nil {
		return fmt.Errorf("write parquet: %w", err)
	}
	return nil
}
