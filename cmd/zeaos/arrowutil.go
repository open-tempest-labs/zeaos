package main

import (
	"fmt"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// recordSliceReader implements array.RecordReader over a slice of retained Arrow
// records. Each call to newRecordSliceReader produces a fresh, independently
// iterable reader — creating it is O(1) with no data copy.
type recordSliceReader struct {
	refCount int64
	schema   *arrow.Schema
	records  []arrow.Record
	cur      int
}

func newRecordSliceReader(schema *arrow.Schema, records []arrow.Record) array.RecordReader {
	return &recordSliceReader{schema: schema, records: records, cur: -1, refCount: 1}
}

func (r *recordSliceReader) Schema() *arrow.Schema { return r.schema }

func (r *recordSliceReader) Next() bool {
	r.cur++
	return r.cur < len(r.records)
}

func (r *recordSliceReader) Record() arrow.Record {
	if r.cur < 0 || r.cur >= len(r.records) {
		return nil
	}
	return r.records[r.cur]
}

func (r *recordSliceReader) Err() error         { return nil }
func (r *recordSliceReader) RecordBatch() arrow.Record { return r.Record() }

func (r *recordSliceReader) Retain() { atomic.AddInt64(&r.refCount, 1) }

// Release decrements the ref count. Records are owned by TableEntry, not here.
func (r *recordSliceReader) Release() { atomic.AddInt64(&r.refCount, -1) }

// recordRowCount sums row counts across all Arrow records.
func recordRowCount(records []arrow.Record) int64 {
	var n int64
	for _, r := range records {
		n += r.NumRows()
	}
	return n
}


var scanSeq int64

// scanID returns a unique internal name for a transient DuckDB Arrow scan.
func scanID() string {
	return fmt.Sprintf("_z%x", atomic.AddInt64(&scanSeq, 1))
}
