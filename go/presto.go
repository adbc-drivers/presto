// Copyright (c) 2025 ADBC Drivers Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//         http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package presto

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	// register the "presto" driver with database/sql
	_ "github.com/prestodb/presto-go-client/v2"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
)

// prestoTypeConverter provides Presto-specific type conversion enhancements
type prestoTypeConverter struct {
	sqlwrapper.DefaultTypeConverter
}

const vendorName = "Presto"

var typeConverter = &prestoTypeConverter{
	DefaultTypeConverter: sqlwrapper.DefaultTypeConverter{VendorName: vendorName},
}

// ConvertRawColumnType implements TypeConverter with Presto-specific enhancements.
//
// Unlike other clients, the presto-go-client does not expose precision/scale
// or nullability metadata through database/sql, so conversions rely on the
// (normalized, upper-case) database type name and Presto's fixed precisions:
// Presto timestamps and times always have millisecond precision.
func (m *prestoTypeConverter) ConvertRawColumnType(colType sqlwrapper.ColumnType) (arrow.DataType, bool, arrow.Metadata, error) {
	typeName := strings.ToUpper(colType.DatabaseTypeName)

	// The presto go client does not provide nullability metadata, so assume
	// every column is always nullable.
	colType.Nullable = true

	metadataMap := map[string]string{
		sqlwrapper.MetaKeyDatabaseTypeName: colType.DatabaseTypeName,
		sqlwrapper.MetaKeyColumnName:       colType.Name,
	}

	switch typeName {
	case "TIMESTAMP":
		// Presto timestamps have millisecond precision (no parameterized
		// precision like Trino).  Timezone-naive.
		metadata := arrow.MetadataFrom(metadataMap)
		return &arrow.TimestampType{Unit: arrow.Millisecond}, colType.Nullable, metadata, nil

	case "TIMESTAMP WITH TIME ZONE":
		metadata := arrow.MetadataFrom(metadataMap)
		return &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: "UTC"}, colType.Nullable, metadata, nil

	case "TIME", "TIME WITH TIME ZONE":
		// Presto times have millisecond precision.  The client scans TIME
		// WITH TIME ZONE to a plain time.Time as well, so both map to
		// time32[ms]; the original type is preserved in metadata.
		metadata := arrow.MetadataFrom(metadataMap)
		return &arrow.Time32Type{Unit: arrow.Millisecond}, colType.Nullable, metadata, nil

	case "REAL":
		// Presto uses REAL for float32/single precision
		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.PrimitiveTypes.Float32, colType.Nullable, metadata, nil

	case "DECIMAL":
		// The presto go client does not expose decimal precision/scale, and
		// scans decimals as strings for precision safety.  Map to string to
		// avoid lossy conversions; the database type is kept in metadata.
		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, colType.Nullable, metadata, nil

	case "INTERVAL YEAR TO MONTH":
		// Presto's INTERVAL YEAR TO MONTH is returned as a string (e.g., "2-6"
		// for 2 years 6 months).  Map to Arrow's MonthDayNanoInterval (with
		// days=0, nanoseconds=0) since PyArrow doesn't have year-month
		// interval support.
		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.FixedWidthTypes.MonthDayNanoInterval, colType.Nullable, metadata, nil

	case "INTERVAL DAY TO SECOND":
		// The presto go client scans INTERVAL DAY TO SECOND as time.Duration.
		// Map to Arrow's MonthDayNanoInterval (with months=0).
		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.FixedWidthTypes.MonthDayNanoInterval, colType.Nullable, metadata, nil

	case "IPADDRESS":
		// Presto's IPADDRESS type stores IPv4 and IPv6 addresses.
		// Returned as string representation (e.g., "192.168.1.1", "::1").
		// Arrow doesn't have a native IP address type, so map to string.
		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, colType.Nullable, metadata, nil

	case "UUID":
		// Presto's UUID type represents universally unique identifiers
		// (RFC 4122), returned as a string in standard format.
		metadata := arrow.MetadataFrom(metadataMap)
		return extensions.NewUUIDType(), colType.Nullable, metadata, nil

	case "ARRAY", "MAP", "ROW":
		// The presto go client scans complex types as JSON strings.  Map to
		// string; the original type is preserved in metadata.
		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.BinaryTypes.String, colType.Nullable, metadata, nil

	case "UNKNOWN":
		// Presto's type for untyped NULL literals.
		metadata := arrow.MetadataFrom(metadataMap)
		return arrow.Null, colType.Nullable, metadata, nil
	}

	// For all other types, fall back to default conversion
	return m.DefaultTypeConverter.ConvertRawColumnType(colType)
}

// CreateInserter creates Presto-specific inserters bound to builders
func (m *prestoTypeConverter) CreateInserter(field *arrow.Field, builder array.Builder) (sqlwrapper.Inserter, error) {
	// Check for Presto-specific types first
	switch fieldType := field.Type.(type) {
	case *arrow.TimestampType:
		// Handle Presto's timezone-naive TIMESTAMP specially
		if fieldType.TimeZone == "" {
			return &prestoTimestampInserter{
				builder: builder.(*array.TimestampBuilder),
				unit:    fieldType.Unit,
			}, nil
		}
		// For timezone-aware timestamps, fall back to default
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	case *arrow.Date32Type:
		return &date32Inserter{builder: builder.(*array.Date32Builder)}, nil
	case *arrow.StringType:
		dbTypeName, exists := field.Metadata.GetValue(sqlwrapper.MetaKeyDatabaseTypeName)
		if exists {
			switch strings.ToUpper(dbTypeName) {
			case "IPADDRESS":
				return &prestoStringInserter{builder: builder.(*array.StringBuilder)}, nil
			case "ARRAY", "MAP", "ROW":
				return &prestoStringInserter{
					builder:     builder.(*array.StringBuilder),
					compactJSON: true,
				}, nil
			}
		}
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	case *arrow.MonthDayNanoIntervalType:
		// Interval types require custom inserters
		dbTypeName, exists := field.Metadata.GetValue(sqlwrapper.MetaKeyDatabaseTypeName)
		if !exists {
			return nil, fmt.Errorf("no database type name in field metadata for interval type")
		}
		switch dbTypeName {
		case "INTERVAL YEAR TO MONTH":
			return &yearToMonthIntervalInserter{
				builder: builder.(*array.MonthDayNanoIntervalBuilder),
			}, nil
		case "INTERVAL DAY TO SECOND":
			return &dayToSecondIntervalInserter{
				builder: builder.(*array.MonthDayNanoIntervalBuilder),
			}, nil
		default:
			return nil, fmt.Errorf("unsupported interval type: %s", dbTypeName)
		}
	default:
		// For all other types, use default inserter
		return m.DefaultTypeConverter.CreateInserter(field, builder)
	}
}

// prestoStringInserter removes the extra JSON string encoding added by the
// Presto client's fallback conversion for IPADDRESS and complex values.
type prestoStringInserter struct {
	builder     *array.StringBuilder
	compactJSON bool
}

func (ins *prestoStringInserter) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	value, ok := unwrapped.(string)
	if !ok {
		return fmt.Errorf("expected string for Presto string-backed type, got %T", sqlValue)
	}

	var decoded string
	if err := json.Unmarshal([]byte(value), &decoded); err == nil {
		value = decoded
	}
	if ins.compactJSON {
		var compact bytes.Buffer
		if err := json.Compact(&compact, []byte(value)); err == nil {
			value = compact.String()
		}
	}
	ins.builder.Append(value)
	return nil
}

func unwrap(val any) (any, error) {
	if v, ok := val.(driver.Valuer); ok {
		return v.Value()
	}
	return val, nil
}

// prestoTimestampInserter handles Presto's timezone-naive TIMESTAMP specially
type prestoTimestampInserter struct {
	builder *array.TimestampBuilder
	unit    arrow.TimeUnit
}

func (ins *prestoTimestampInserter) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := unwrapped.(time.Time)
	if !ok {
		return fmt.Errorf("expected time.Time for timestamp, got %T", sqlValue)
	}

	// For Presto's naive timestamps, treat the time.Time as wall-clock time.
	// Create a new time.Time in UTC with the same wall-clock values.
	// This preserves the naive semantics while working with Arrow's expectations
	naiveTime := time.Date(
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond(),
		time.UTC, // Force UTC to prevent timezone conversion
	)

	ins.builder.AppendTime(naiveTime)
	return nil
}

// Date inserters
type date32Inserter struct {
	builder *array.Date32Builder
}

func (ins *date32Inserter) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	t, ok := unwrapped.(time.Time)
	if !ok {
		return fmt.Errorf("expected time.Time for date32 inserter, got %T", sqlValue)
	}

	// Convert to date without timezone conversion
	// Extract just the date components and calculate days since epoch manually
	year, month, day := t.Date()
	utcDate := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	val := arrow.Date32FromTime(utcDate)

	ins.builder.Append(val)
	return nil
}

// yearToMonthIntervalInserter handles INTERVAL YEAR TO MONTH values
type yearToMonthIntervalInserter struct {
	builder *array.MonthDayNanoIntervalBuilder
}

func (ins *yearToMonthIntervalInserter) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	// Interval comes from Presto as a string
	intervalStr, ok := unwrapped.(string)
	if !ok {
		return fmt.Errorf("expected string for interval, got %T", sqlValue)
	}

	interval, err := parseYearToMonth(intervalStr)
	if err != nil {
		return fmt.Errorf("failed to parse YEAR TO MONTH interval '%s': %w", intervalStr, err)
	}

	ins.builder.Append(interval)
	return nil
}

// parseYearToMonth parses "2-6" format (2 years 6 months) to MonthDayNanoInterval
func parseYearToMonth(intervalStr string) (arrow.MonthDayNanoInterval, error) {
	negative := false
	if rest, ok := strings.CutPrefix(intervalStr, "-"); ok {
		negative = true
		intervalStr = rest
	}

	parts := strings.Split(intervalStr, "-")
	if len(parts) != 2 {
		return arrow.MonthDayNanoInterval{}, fmt.Errorf("invalid YEAR TO MONTH format, expected 'Y-M'")
	}

	years, err := strconv.Atoi(parts[0])
	if err != nil {
		return arrow.MonthDayNanoInterval{}, fmt.Errorf("invalid years: %w", err)
	}

	months, err := strconv.Atoi(parts[1])
	if err != nil {
		return arrow.MonthDayNanoInterval{}, fmt.Errorf("invalid months: %w", err)
	}

	totalMonths := int32(years*12 + months)
	if negative {
		totalMonths = -totalMonths
	}
	return arrow.MonthDayNanoInterval{
		Months:      totalMonths,
		Days:        0,
		Nanoseconds: 0,
	}, nil
}

// dayToSecondIntervalInserter handles INTERVAL DAY TO SECOND values.
// The presto go client scans these as time.Duration.
type dayToSecondIntervalInserter struct {
	builder *array.MonthDayNanoIntervalBuilder
}

func (ins *dayToSecondIntervalInserter) AppendValue(sqlValue any) error {
	unwrapped, err := unwrap(sqlValue)
	if err != nil {
		return err
	}
	if unwrapped == nil {
		ins.builder.AppendNull()
		return nil
	}

	d, ok := unwrapped.(time.Duration)
	if !ok {
		return fmt.Errorf("expected time.Duration for DAY TO SECOND interval, got %T", sqlValue)
	}

	// Split the duration into whole days plus a sub-day nanosecond remainder.
	days := d / (24 * time.Hour)
	remainder := d % (24 * time.Hour)

	ins.builder.Append(arrow.MonthDayNanoInterval{
		Months:      0,
		Days:        int32(days),
		Nanoseconds: remainder.Nanoseconds(),
	})
	return nil
}

// ConvertArrowToGo implements Presto-specific Arrow value to Go value conversion.
//
// The presto go client interpolates parameters client-side and supports only
// nil, int64, float64, bool, string, []byte, time.Time and time.Duration, so
// other types are converted to one of those representations here.  Types that
// would lose their SQL type through a plain literal (e.g. decimals rendered
// as strings) are paired with CAST placeholders in getParameterPlaceholder.
func (m *prestoTypeConverter) ConvertArrowToGo(arrowArray arrow.Array, index int, field *arrow.Field) (any, error) {
	if arrowArray.IsNull(index) {
		return nil, nil
	}

	switch a := arrowArray.(type) {
	case *array.Time32:
		// Render as a string; combined with a CAST(? AS TIME) placeholder.
		timeType := a.DataType().(*arrow.Time32Type)
		t := a.Value(index).ToTime(timeType.Unit)
		return t.Format("15:04:05.000"), nil

	case *array.Time64:
		// Render as a string; combined with a CAST(? AS TIME) placeholder.
		timeType := a.DataType().(*arrow.Time64Type)
		t := a.Value(index).ToTime(timeType.Unit)
		return t.Format("15:04:05.000000000"), nil

	case *array.Timestamp:
		// Timezone-aware timestamps are rendered as strings with an explicit
		// zone; combined with a CAST(? AS TIMESTAMP WITH TIME ZONE)
		// placeholder.  This avoids session-timezone-dependent semantics of
		// naive timestamp literals.  Timezone-naive timestamps use the
		// default conversion (time.Time).
		tsType := a.DataType().(*arrow.TimestampType)
		if tsType.TimeZone != "" {
			toTime, err := tsType.GetToTimeFunc()
			if err != nil {
				return nil, err
			}
			return toTime(a.Value(index)).UTC().Format("2006-01-02 15:04:05.000 UTC"), nil
		}
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)

	case *array.Decimal32:
		// Render as a string; combined with a CAST(? AS DECIMAL) placeholder.
		decimalType := a.DataType().(*arrow.Decimal32Type)
		val := a.Value(index)
		return formatDecimalString(big.NewInt(int64(val)), decimalType.Scale), nil

	case *array.Decimal64:
		decimalType := a.DataType().(*arrow.Decimal64Type)
		val := a.Value(index)
		return formatDecimalString(big.NewInt(int64(val)), decimalType.Scale), nil

	case *array.Decimal128:
		decimalType := a.DataType().(*arrow.Decimal128Type)
		val := a.Value(index)
		return formatDecimalString(val.BigInt(), decimalType.Scale), nil

	case *array.Decimal256:
		decimalType := a.DataType().(*arrow.Decimal256Type)
		val := a.Value(index)
		return formatDecimalString(val.BigInt(), decimalType.Scale), nil

	case *array.Float16:
		// The presto go client only supports float64 parameters
		return a.Value(index).Float32(), nil

	case *array.Float32:
		// Fixed-point formatting expands extreme values into integer-looking
		// literals that Presto rejects.  A quoted scientific representation,
		// combined with CAST(? AS REAL), handles the full Arrow range.
		return strconv.FormatFloat(float64(a.Value(index)), 'g', -1, 32), nil

	case *array.Float64:
		return strconv.FormatFloat(a.Value(index), 'g', -1, 64), nil

	case *array.Int64:
		// In particular, -9223372036854775808 cannot be expressed as a plain
		// Presto literal: the positive token overflows before unary minus is
		// applied.  Cast from a string instead.
		value := a.Value(index)
		if value == -1<<63 {
			return strconv.FormatInt(value, 10), nil
		}
		return int64(value), nil

	case *array.FixedSizeBinary:
		// Check metadata for UUID extension type indication
		if extName, exists := field.Metadata.GetValue("ARROW:extension:name"); exists && extName == "arrow.uuid" {
			binaryValue := a.Value(index)

			uuidVal, err := uuid.FromBytes(binaryValue)
			if err != nil {
				return nil, err
			}
			return uuidVal.String(), nil
		}
		// For non-UUID fixed-size binary, let default converter handle it for proper varbinary conversion
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)

	default:
		// For all other types, use default conversion
		return m.DefaultTypeConverter.ConvertArrowToGo(arrowArray, index, field)
	}
}

// formatDecimalString converts a big.Int with scale to a plain decimal string
func formatDecimalString(value *big.Int, scale int32) string {
	// scale means divide by 10^scale
	scaleFactor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil)
	rat := new(big.Rat).SetFrac(value, scaleFactor)

	digits := max(int(scale), 0)
	return rat.FloatString(digits)
}

// prestoConnectionFactory creates Presto connections
type prestoConnectionFactory struct{}

// CreateConnection implements sqlwrapper.ConnectionFactory
func (f *prestoConnectionFactory) CreateConnection(
	ctx context.Context,
	conn *sqlwrapper.ConnectionImplBase,
) (sqlwrapper.ConnectionImpl, error) {
	// Wrap the pre-built sqlwrapper connection with Presto-specific functionality
	return &prestoConnectionImpl{
		ConnectionImplBase: conn,
	}, nil
}

func (f *prestoConnectionFactory) CreateStatement(stmt *sqlwrapper.StatementImplBase) (sqlwrapper.StatementImpl, error) {
	return &prestoStatement{
		StatementImplBase: stmt,
	}, nil
}

// NewDriver constructs the ADBC Driver for "presto".
func NewDriver(alloc memory.Allocator) driverbase.DriverWithContext {
	factory := &prestoConnectionFactory{}
	driver := sqlwrapper.NewDriver(alloc, "presto", vendorName, NewPrestoDBFactory()).
		WithConnectionFactory(factory).
		WithStatementFactory(factory).
		WithErrorInspector(PrestoErrorInspector{})
	driver.DriverInfo.MustRegister(map[adbc.InfoCode]any{
		adbc.InfoDriverName:      "ADBC Driver Foundry Driver for Presto",
		adbc.InfoVendorSql:       true,
		adbc.InfoVendorSubstrait: false,
	})

	return driver
}
