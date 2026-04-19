//go:build duckdb_arrow

package main

import (
	"database/sql/driver"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// arrowConnType is the live DuckDB Arrow accessor (requires duckdb_arrow build tag).
type arrowConnType = duckdb.Arrow

func newArrowFromConn(dc driver.Conn) (*arrowConnType, error) {
	return duckdb.NewArrowFromConn(dc)
}
