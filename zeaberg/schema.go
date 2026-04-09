package zeaberg

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	iceberg "github.com/apache/iceberg-go"
)

// SchemaFromArrow converts an Arrow schema to an Iceberg schema.
// Field IDs start at 1 and increment sequentially.
func SchemaFromArrow(s *arrow.Schema) (*iceberg.Schema, error) {
	fields := make([]iceberg.NestedField, 0, s.NumFields())
	for i := 0; i < s.NumFields(); i++ {
		f := s.Field(i)
		iceType, err := arrowTypeToIceberg(f.Type)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.Name, err)
		}
		fields = append(fields, iceberg.NestedField{
			ID:       i + 1,
			Name:     f.Name,
			Type:     iceType,
			Required: !f.Nullable,
		})
	}
	return iceberg.NewSchema(0, fields...), nil
}

func arrowTypeToIceberg(dt arrow.DataType) (iceberg.Type, error) {
	switch dt.ID() {
	case arrow.BOOL:
		return iceberg.BooleanType{}, nil
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.UINT8, arrow.UINT16:
		return iceberg.Int32Type{}, nil
	case arrow.INT64, arrow.UINT32, arrow.UINT64:
		return iceberg.Int64Type{}, nil
	case arrow.FLOAT32:
		return iceberg.Float32Type{}, nil
	case arrow.FLOAT64:
		return iceberg.Float64Type{}, nil
	case arrow.DECIMAL128:
		dt128 := dt.(*arrow.Decimal128Type)
		return iceberg.DecimalTypeOf(int(dt128.Precision), int(dt128.Scale)), nil
	case arrow.DECIMAL256:
		return iceberg.DecimalTypeOf(38, 9), nil
	case arrow.DATE32, arrow.DATE64:
		return iceberg.DateType{}, nil
	case arrow.TIME32, arrow.TIME64:
		return iceberg.TimeType{}, nil
	case arrow.TIMESTAMP:
		ts := dt.(*arrow.TimestampType)
		if ts.TimeZone != "" {
			return iceberg.TimestampTzType{}, nil
		}
		return iceberg.TimestampType{}, nil
	case arrow.STRING, arrow.LARGE_STRING:
		return iceberg.StringType{}, nil
	case arrow.BINARY, arrow.LARGE_BINARY:
		return iceberg.BinaryType{}, nil
	case arrow.FIXED_SIZE_BINARY:
		fsb := dt.(*arrow.FixedSizeBinaryType)
		return iceberg.FixedTypeOf(fsb.ByteWidth), nil
	default:
		return iceberg.StringType{}, nil
	}
}
