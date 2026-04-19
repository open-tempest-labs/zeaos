//go:build !duckdb_arrow

package main

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// arrowConnType is a stub when the duckdb_arrow build tag is absent.
// All methods return an error; NewSession will fail at arrow init before
// any of these are called.
type arrowConnType struct{}

func newArrowFromConn(_ driver.Conn) (*arrowConnType, error) {
	return nil, fmt.Errorf("zeaos must be built with -tags duckdb_arrow")
}

func (a *arrowConnType) QueryContext(_ context.Context, _ string, _ ...any) (array.RecordReader, error) {
	return nil, fmt.Errorf("zeaos must be built with -tags duckdb_arrow")
}

func (a *arrowConnType) RegisterView(_ array.RecordReader, _ string) (func(), error) {
	return nil, fmt.Errorf("zeaos must be built with -tags duckdb_arrow")
}
