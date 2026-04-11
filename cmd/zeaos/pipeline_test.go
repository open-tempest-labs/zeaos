package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestPipelineS3Iceberg exercises the full load → filter → push --iceberg →
// iceberg verify pipeline against a real MinIO instance. It only runs when
// ZEA_TEST_S3_ENDPOINT is set, which the docker-compose test harness provides.
func TestPipelineS3Iceberg(t *testing.T) {
	if os.Getenv("ZEA_TEST_S3_ENDPOINT") == "" {
		t.Skip("ZEA_TEST_S3_ENDPOINT not set — skipping S3 integration test")
	}

	cases := []struct {
		name      string
		csvRows   []string // header + data rows
		whereExpr string   // pipe | where expression (goes through arrowfilter, not DuckDB Arrow scan)
		wantRows  int64    // expected rows after filter
	}{
		{
			name: "sales_pipeline",
			csvRows: []string{
				"id,product,amount",
				"1,widget,15",
				"2,gadget,8",
				"3,widget,22",
				"4,doohickey,5",
				"5,gadget,30",
			},
			// Pipe | where uses Arrow compute directly — predicate evaluation is
			// reliable for integer columns. zeaql WHERE over an Arrow scan view
			// does not apply predicates correctly in the DuckDB Arrow C Data
			// Interface path.
			whereExpr: "id > 2",
			wantRows:  3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Use a timestamped schema so parallel or repeated runs don't collide.
			schema := fmt.Sprintf("pipeline_test_%d", time.Now().UnixMilli())

			// Write the CSV to a temp file the session can load.
			csvFile, err := os.CreateTemp("", "zeaos-test-*.csv")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(csvFile.Name())
			if _, err := csvFile.WriteString(strings.Join(tc.csvRows, "\n") + "\n"); err != nil {
				t.Fatal(err)
			}
			csvFile.Close()

			s, err := NewSession()
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			defer s.Close()

			// Use table names that won't collide with an existing session.
			rawName := fmt.Sprintf("_test_raw_%d", time.Now().UnixMilli())
			resultName := fmt.Sprintf("_test_result_%d", time.Now().UnixMilli())
			defer func() {
				_ = s.Drop(rawName)
				_ = s.Drop(resultName)
			}()

			// Step 1: load CSV.
			if err := execLine(fmt.Sprintf("%s = load %s", rawName, csvFile.Name()), s); err != nil {
				t.Fatalf("load: %v", err)
			}
			raw, err := s.Get(rawName)
			if err != nil {
				t.Fatalf("get raw: %v", err)
			}
			wantRaw := int64(len(tc.csvRows) - 1) // subtract header
			if raw.RowCount != wantRaw {
				t.Errorf("raw row count: got %d want %d", raw.RowCount, wantRaw)
			}

			// Step 2: pipe filter — uses arrowfilter (Arrow compute), not DuckDB
			// Arrow scan, so the predicate is applied correctly.
			filterCmd := fmt.Sprintf("%s = %s | where %s", resultName, rawName, tc.whereExpr)
			if err := execLine(filterCmd, s); err != nil {
				t.Fatalf("filter: %v", err)
			}
			result, err := s.Get(resultName)
			if err != nil {
				t.Fatalf("get result: %v", err)
			}
			if result.RowCount != tc.wantRows {
				t.Errorf("filtered row count: got %d want %d", result.RowCount, tc.wantRows)
			}

			// Step 3: push as Iceberg to MinIO. Pass --schema to skip interactive prompt.
			pushCmd := fmt.Sprintf("push %s --target zea://s3-data --schema %s --iceberg", resultName, schema)
			if err := execLine(pushCmd, s); err != nil {
				t.Fatalf("push: %v", err)
			}
			result, _ = s.Get(resultName)
			if len(result.PushRecords) == 0 {
				t.Fatal("no push record after push")
			}
			rec := result.PushRecords[len(result.PushRecords)-1]
			if rec.Format != "iceberg" {
				t.Errorf("push format: got %q want iceberg", rec.Format)
			}
			if rec.RowCount != tc.wantRows {
				t.Errorf("push row count: got %d want %d", rec.RowCount, tc.wantRows)
			}

			// Step 4: verify Iceberg snapshot integrity.
			if err := execLine(fmt.Sprintf("iceberg verify %s", resultName), s); err != nil {
				t.Fatalf("iceberg verify: %v", err)
			}

			// Step 5: confirm verify cache was written.
			cacheDir := filepath.Join(s.Dir, "verify-cache")
			if _, err := os.Stat(cacheDir); err != nil {
				t.Errorf("verify cache not written: %v", err)
			}
		})
	}
}
