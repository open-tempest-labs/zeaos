package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/open-tempest-labs/zeashell/zeaframe"
)

// filterArrowRecords applies where ops directly against Arrow records using
// zeaframe's expression parser, bypassing DuckDB's Arrow scan predicate
// pushdown bug for integer columns.
func filterArrowRecords(schema *arrow.Schema, records []arrow.Record, whereOps []PipeOp) ([]arrow.Record, error) {
	parts := make([]string, len(whereOps))
	for i, op := range whereOps {
		parts[i] = op.Args
	}
	expr, err := zeaframe.ParseExpression(strings.Join(parts, " AND "))
	if err != nil {
		return nil, fmt.Errorf("filter: %w", err)
	}

	ctx := context.Background()
	var out []arrow.Record
	for _, rec := range records {
		filtered, err := filterRecord(ctx, schema, rec, expr)
		if err != nil {
			for _, r := range out {
				r.Release()
			}
			return nil, err
		}
		if filtered.NumRows() > 0 {
			out = append(out, filtered)
		} else {
			filtered.Release()
		}
	}
	return out, nil
}

// filterRecord builds a boolean mask for a single Arrow record batch and
// applies it via compute.FilterRecordBatch.
func filterRecord(ctx context.Context, schema *arrow.Schema, rec arrow.Record, expr *zeaframe.Expression) (arrow.Record, error) {
	n := rec.NumRows()
	bldr := array.NewBooleanBuilder(memory.DefaultAllocator)
	defer bldr.Release()
	bldr.Reserve(int(n))

	for i := int64(0); i < n; i++ {
		match, err := evalExpr(schema, rec, i, expr)
		if err != nil {
			return nil, err
		}
		bldr.Append(match)
	}

	mask := bldr.NewBooleanArray()
	defer mask.Release()

	opts := compute.DefaultFilterOptions()
	return compute.FilterRecordBatch(ctx, rec, mask, opts)
}

// evalExpr recursively evaluates a zeaframe Expression against a single row.
func evalExpr(schema *arrow.Schema, rec arrow.Record, row int64, expr *zeaframe.Expression) (bool, error) {
	if !expr.IsLeaf {
		left, err := evalExpr(schema, rec, row, expr.Left)
		if err != nil {
			return false, err
		}
		// Short-circuit evaluation
		if expr.Operator == "AND" && !left {
			return false, nil
		}
		if expr.Operator == "OR" && left {
			return true, nil
		}
		right, err := evalExpr(schema, rec, row, expr.Right)
		if err != nil {
			return false, err
		}
		switch expr.Operator {
		case "AND":
			return left && right, nil
		case "OR":
			return left || right, nil
		}
		return false, fmt.Errorf("unknown logical operator: %s", expr.Operator)
	}

	indices := schema.FieldIndices(expr.Column)
	if len(indices) == 0 {
		return false, fmt.Errorf("column %q not found", expr.Column)
	}
	col := rec.Column(indices[0])
	dtype := schema.Field(indices[0]).Type

	if col.IsNull(int(row)) {
		return false, nil
	}

	return compareArrowValue(col, dtype, row, expr.Operator, expr.Value)
}

// compareArrowValue extracts the typed value at row and compares it to target.
func compareArrowValue(col arrow.Array, dtype arrow.DataType, row int64, op string, target interface{}) (bool, error) {
	switch dtype.ID() {
	case arrow.INT64:
		if op == "~" {
			return false, fmt.Errorf("operator ~ requires a string column; ROCK_TYPE is int64 — use = for equality")
		}
		lv := col.(*array.Int64).Value(int(row))
		rv, err := toArrowInt64(target)
		if err != nil {
			return false, err
		}
		return compareOrdered(lv, rv, op)
	case arrow.INT32:
		lv := int64(col.(*array.Int32).Value(int(row)))
		rv, err := toArrowInt64(target)
		if err != nil {
			return false, err
		}
		return compareOrdered(lv, rv, op)
	case arrow.INT16:
		lv := int64(col.(*array.Int16).Value(int(row)))
		rv, err := toArrowInt64(target)
		if err != nil {
			return false, err
		}
		return compareOrdered(lv, rv, op)
	case arrow.INT8:
		lv := int64(col.(*array.Int8).Value(int(row)))
		rv, err := toArrowInt64(target)
		if err != nil {
			return false, err
		}
		return compareOrdered(lv, rv, op)
	case arrow.UINT64:
		lv := int64(col.(*array.Uint64).Value(int(row)))
		rv, err := toArrowInt64(target)
		if err != nil {
			return false, err
		}
		return compareOrdered(lv, rv, op)
	case arrow.UINT32:
		lv := int64(col.(*array.Uint32).Value(int(row)))
		rv, err := toArrowInt64(target)
		if err != nil {
			return false, err
		}
		return compareOrdered(lv, rv, op)
	case arrow.FLOAT64:
		lv := col.(*array.Float64).Value(int(row))
		rv, err := toArrowFloat64(target)
		if err != nil {
			return false, err
		}
		return compareOrdered(lv, rv, op)
	case arrow.FLOAT32:
		lv := float64(col.(*array.Float32).Value(int(row)))
		rv, err := toArrowFloat64(target)
		if err != nil {
			return false, err
		}
		return compareOrdered(lv, rv, op)
	case arrow.STRING, arrow.LARGE_STRING:
		var lv string
		if dtype.ID() == arrow.STRING {
			lv = col.(*array.String).Value(int(row))
		} else {
			lv = col.(*array.LargeString).Value(int(row))
		}
		// Trim trailing spaces — Parquet fixed-width string fields often pad.
		lv = strings.TrimRight(lv, " ")
		rv := strings.TrimRight(fmt.Sprintf("%v", target), " ")
		if op == "~" {
			matched, err := regexp.MatchString(rv, lv)
			return matched, err
		}
		return compareOrdered(lv, rv, op)
	case arrow.BOOL:
		lv := col.(*array.Boolean).Value(int(row))
		rv, ok := target.(bool)
		if !ok {
			return false, fmt.Errorf("cannot compare bool column to %T", target)
		}
		switch op {
		case "=":
			return lv == rv, nil
		case "!=":
			return lv != rv, nil
		}
		return false, fmt.Errorf("operator %q not supported for bool", op)
	default:
		return false, fmt.Errorf("unsupported column type %s for Arrow filter", dtype)
	}
}

// compareOrdered handles =, !=, <, <=, >, >= for any ordered type.
func compareOrdered[T interface {
	int64 | float64 | string
}](lv, rv T, op string) (bool, error) {
	switch op {
	case "=":
		return lv == rv, nil
	case "!=":
		return lv != rv, nil
	case ">":
		return lv > rv, nil
	case ">=":
		return lv >= rv, nil
	case "<":
		return lv < rv, nil
	case "<=":
		return lv <= rv, nil
	}
	return false, fmt.Errorf("unsupported operator %q", op)
}

func toArrowInt64(v interface{}) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case float64:
		return int64(x), nil
	}
	return 0, fmt.Errorf("cannot convert %T to int64", v)
}

func toArrowFloat64(v interface{}) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case int64:
		return float64(x), nil
	}
	return 0, fmt.Errorf("cannot convert %T to float64", v)
}
