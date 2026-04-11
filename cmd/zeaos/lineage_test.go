package main

import (
	"testing"
)

func TestInferSQLParent(t *testing.T) {
	tables := []string{"trips", "avg_tip", "long_trips", "zone_revenue", "t"}

	cases := []struct {
		name   string
		sql    string
		want   string
	}{
		{
			name: "simple FROM",
			sql:  "SELECT * FROM trips WHERE x > 1",
			want: "trips",
		},
		{
			name: "alias shadowing — column alias same as table name should not win",
			sql:  "SELECT payment_type, COUNT(*) AS trips FROM trips GROUP BY payment_type",
			want: "trips",
		},
		{
			name: "FROM long_trips",
			sql:  "SELECT PULocationID, SUM(fare_amount) FROM long_trips GROUP BY PULocationID",
			want: "long_trips",
		},
		{
			name: "AS avg_tip alias should not self-parent",
			sql:  "SELECT payment_type, AVG(tip) AS avg_tip FROM trips GROUP BY payment_type",
			want: "trips",
		},
		{
			name: "iceberg_scan path containing table name is not a parent",
			sql:  "SELECT * FROM iceberg_scan('/exports/zea_exports/avg_tip')",
			want: "",
		},
		{
			name: "JOIN",
			sql:  "SELECT a.x FROM trips JOIN long_trips ON trips.id = long_trips.id",
			want: "trips",
		},
		{
			name: "no session table referenced",
			sql:  "SELECT 1 + 1",
			want: "",
		},
		{
			name: "single-char table name t not spuriously matched",
			sql:  "SELECT * FROM trips LIMIT 10",
			want: "trips",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := inferSQLParent(c.sql, tables)
			if got != c.want {
				t.Errorf("inferSQLParent(%q)\n  got  %q\n  want %q", c.sql, got, c.want)
			}
		})
	}
}

func TestSQLOps(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		wantOp  string
	}{
		{
			name:   "plain sql",
			sql:    "SELECT * FROM trips",
			wantOp: "sql",
		},
		{
			name:   "iceberg_scan single quotes",
			sql:    "SELECT * FROM iceberg_scan('/exports/zea_exports/avg_tip')",
			wantOp: "iceberg_scan(zea_exports/avg_tip)",
		},
		{
			name:   "iceberg_scan double quotes",
			sql:    `SELECT * FROM iceberg_scan("/exports/zea_exports/avg_tip")`,
			wantOp: "iceberg_scan(zea_exports/avg_tip)",
		},
		{
			name:   "iceberg_scan deep path uses last two components",
			sql:    "SELECT * FROM iceberg_scan('/a/b/c/d/schema/table_name')",
			wantOp: "iceberg_scan(schema/table_name)",
		},
		{
			name:   "iceberg_scan uppercase",
			sql:    "SELECT * FROM ICEBERG_SCAN('/exports/zea_exports/my_table')",
			wantOp: "iceberg_scan(zea_exports/my_table)",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ops := sqlOps(c.sql)
			if len(ops) != 1 {
				t.Fatalf("len(ops) = %d, want 1", len(ops))
			}
			if ops[0] != c.wantOp {
				t.Errorf("sqlOps(%q)\n  got  %q\n  want %q", c.sql, ops[0], c.wantOp)
			}
		})
	}
}
