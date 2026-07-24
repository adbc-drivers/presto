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
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	sqlwrapper "github.com/adbc-drivers/driverbase-go/sqlwrapper"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

const (
	// Presto's default maximum query size limit (1 million characters,
	// query.max-length)
	PrestoMaxQuerySizeBytes = 1_000_000
)

// prestoConnectionImpl extends sqlwrapper connection with DbObjectsEnumerator
type prestoConnectionImpl struct {
	*sqlwrapper.ConnectionImplBase // Embed sqlwrapper connection for all standard functionality

	version string
}

// implements BulkIngester interface
var _ sqlwrapper.BulkIngester = (*prestoConnectionImpl)(nil)

// implements DbObjectsEnumerator interface
var _ driverbase.DbObjectsEnumerator = (*prestoConnectionImpl)(nil)

// implements CurrentNameSpacer interface
var _ driverbase.CurrentNamespacer = (*prestoConnectionImpl)(nil)

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
//
// PrestoDB has no current_catalog SQL function (unlike Trino), so the driver
// returns the namespace it tracks for this database (initialized from the
// connection URI and updated by SetCurrentCatalog).
func (c *prestoConnectionImpl) GetCurrentCatalog(ctx context.Context) (string, error) {
	catalog, _ := namespaceForDB(c.Db).get()
	return catalog, nil
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *prestoConnectionImpl) GetCurrentDbSchema(ctx context.Context) (string, error) {
	_, schema := namespaceForDB(c.Db).get()
	return schema, nil
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *prestoConnectionImpl) SetCurrentCatalog(ctx context.Context, catalog string) error {
	if catalog == "" {
		return nil // No-op for empty catalog
	}

	// Validate the catalog exists before updating the tracked namespace.
	var count int64
	err := c.Db.QueryRowContext(ctx, "SELECT count(*) FROM system.metadata.catalogs WHERE catalog_name = ?", catalog).Scan(&count)
	if err != nil {
		return c.ErrorHelper.WrapIO(err, "failed to validate catalog %s", catalog)
	}
	if count == 0 {
		return c.ErrorHelper.NotFound("catalog not found: %s", catalog)
	}

	namespaceForDB(c.Db).setCatalog(catalog)
	return nil
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *prestoConnectionImpl) SetCurrentDbSchema(ctx context.Context, schema string) error {
	if schema == "" {
		return nil // No-op for empty schema
	}

	state := namespaceForDB(c.Db)
	catalog, _ := state.get()
	if catalog == "" {
		return c.ErrorHelper.InvalidArgument("cannot set current schema without a current catalog")
	}

	// Validate the schema exists before updating the tracked namespace.
	query := fmt.Sprintf("SELECT count(*) FROM %s.information_schema.schemata WHERE schema_name = ?", quoteIdentifier(catalog))
	var count int64
	err := c.Db.QueryRowContext(ctx, query, schema).Scan(&count)
	if err != nil {
		return c.ErrorHelper.WrapIO(err, "failed to validate schema %s", schema)
	}
	if count == 0 {
		return c.ErrorHelper.NotFound("schema not found: %s.%s", catalog, schema)
	}

	state.setSchema(schema)
	return nil
}

func (c *prestoConnectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if c.version == "" {
		var version string
		if err := c.Conn.QueryRowContext(ctx, "SELECT node_version FROM system.runtime.nodes LIMIT 1").Scan(&version); err != nil {
			return c.ErrorHelper.WrapIO(err, "failed to get version")
		}
		c.version = fmt.Sprintf("Presto %s", version)
	}
	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorVersion, c.version)
}

// GetTableSchema returns the Arrow schema for a Presto table
func (c *prestoConnectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (schema *arrow.Schema, err error) {
	var catalogName, schemaName string

	// Get catalog
	if catalog != nil && *catalog != "" {
		catalogName = *catalog
	} else {
		catalogName, err = c.GetCurrentCatalog(ctx)
		if err != nil {
			return nil, err
		}
	}

	// Get schema
	if dbSchema != nil && *dbSchema != "" {
		schemaName = *dbSchema
	} else {
		schemaName, err = c.GetCurrentDbSchema(ctx)
		if err != nil {
			return nil, err
		}
	}

	qualifiedTableName := fmt.Sprintf("%s.%s.%s", quoteIdentifier(catalogName), quoteIdentifier(schemaName), quoteIdentifier(tableName))

	query := fmt.Sprintf("SELECT * FROM %s WHERE 1=0", qualifiedTableName)
	stmt, err := c.Conn.PrepareContext(ctx, query)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to prepare statement")
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	// Go's database/sql package doesn't provide a direct way to extract
	// column types from a prepared statement without executing it.
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to execute schema query")
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	// Get column types from the result set
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, c.ErrorHelper.WrapInternal(err, "failed to get column types")
	}

	if len(columnTypes) == 0 {
		return nil, c.ErrorHelper.NotFound("table not found: %s", tableName)
	}

	// Convert column types to Arrow fields using the existing type converter
	fields := make([]arrow.Field, len(columnTypes))
	for i, colType := range columnTypes {
		wrappedColType := sqlwrapper.ColumnType{
			Name:             colType.Name(),
			DatabaseTypeName: colType.DatabaseTypeName(),
			Nullable:         true, // Assume every column is always nullable since the presto go client does not provide nullability metadata.
			ScanType:         colType.ScanType(),
		}

		// Add precision and scale if available
		if precision, scale, ok := colType.DecimalSize(); ok {
			p, s := int64(precision), int64(scale)
			wrappedColType.Precision = &p
			wrappedColType.Scale = &s
		} else if length, ok := colType.Length(); ok {
			l := int64(length)
			wrappedColType.Precision = &l
		}

		arrowType, nullable, metadata, err := c.TypeConverter.ConvertRawColumnType(wrappedColType)
		if err != nil {
			return nil, c.ErrorHelper.WrapInternal(err, "failed to convert column type for %s", colType.Name())
		}

		fields[i] = arrow.Field{
			Name:     colType.Name(),
			Type:     arrowType,
			Nullable: nullable,
			Metadata: metadata,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

// QuoteIdentifiers implements BulkIngester
func (c *prestoConnectionImpl) QuoteIdentifiers(parts []string) string {
	return quoteIdentifiers(parts)
}

// GetPlaceholder implements BulkIngester
func (c *prestoConnectionImpl) GetPlaceholder(field *arrow.Field, index int) string {
	return c.getParameterPlaceholder(*field)
}

// Ensure prestoConnectionImpl implements BulkIngester
var _ sqlwrapper.BulkIngester = (*prestoConnectionImpl)(nil)

// ExecuteBulkIngest performs Presto bulk ingest using batched INSERT statements.
func (c *prestoConnectionImpl) ExecuteBulkIngest(ctx context.Context, stmt sqlwrapper.StatementImpl, conn *sqlwrapper.LoggingConn, options *driverbase.BulkIngestOptions, stream array.RecordReader) (rowCount int64, err error) {
	params := stmt.(*prestoStatement).GetAdditionalExecParams()

	schema := stream.Schema()
	qualifiedTable := qualifiedTableName(options.CatalogName, options.SchemaName, options.TableName)
	if err := c.createTableIfNeeded(ctx, conn, qualifiedTable, schema, options, params); err != nil {
		return -1, c.ErrorHelper.WrapIO(err, "failed to create table")
	}

	if options.IngestBatchSize > 0 {
		return sqlwrapper.ExecuteBatchedBulkIngest(
			ctx, stmt, conn, options, stream,
			c.TypeConverter, c, &c.Base().ErrorHelper,
		)
	}

	if options.MaxQuerySizeBytes == 0 {
		options.MaxQuerySizeBytes = PrestoMaxQuerySizeBytes
	}

	// Use Presto-specific batching with accurate serialized size measurement
	return c.executeDynamicBatchedIngest(ctx, conn, qualifiedTable, options, stream, params)
}

// executeDynamicBatchedIngest performs batched INSERT with incremental query building.
//
// Presto has a 1MB query length limit by default. This function builds the
// INSERT query incrementally, checking the actual query length after each
// row. When adding a row would exceed the limit, the current batch is
// executed and a new batch starts.
//
// This implementation builds complete INSERT statements with
// serialized values embedded directly, rather than using parameterized queries.
func (c *prestoConnectionImpl) executeDynamicBatchedIngest(
	ctx context.Context,
	conn *sqlwrapper.LoggingConn,
	qualifiedTable string,
	options *driverbase.BulkIngestOptions,
	stream array.RecordReader,
	params []any,
) (int64, error) {
	var totalRowsInserted int64
	schema := stream.Schema()
	numCols := len(schema.Fields())

	baseQuery := fmt.Sprintf("INSERT INTO %s VALUES ", qualifiedTable)

	for stream.Next() {
		recordBatch := stream.RecordBatch()
		totalRows := int(recordBatch.NumRows())

		var queryBuilder strings.Builder
		queryBuilder.WriteString(baseQuery)
		currentBatchRows := 0
		startRowIdx := 0

		for rowIdx := range totalRows {
			// Serialize all values in this row
			serializedRow := make([]string, numCols)
			for colIdx := range numCols {
				arr := recordBatch.Column(colIdx)
				field := schema.Field(colIdx)

				goValue, err := c.TypeConverter.ConvertArrowToGo(arr, rowIdx, &field)
				if err != nil {
					return totalRowsInserted, c.ErrorHelper.WrapIO(err, "failed to convert value")
				}

				serialized, err := serializeSQLLiteral(goValue)
				if err != nil {
					return totalRowsInserted, c.ErrorHelper.WrapIO(err, "failed to serialize value")
				}

				serializedRow[colIdx] = serialized
			}

			// Build row string: "(val1,val2,val3)" or "CAST(val AS TYPE)" where needed
			rowString := c.buildRowString(schema, serializedRow)

			// Check if adding this row would exceed the limit
			// Add 1 for comma separator (except for first row in batch)
			additionalLength := len(rowString)
			if currentBatchRows > 0 {
				additionalLength += 1 // for comma
			}

			if queryBuilder.Len()+additionalLength > options.MaxQuerySizeBytes && currentBatchRows > 0 {
				// Execute current batch
				rowsInserted, err := c.executeBatch(ctx, conn, queryBuilder.String(), params)
				if err != nil {
					return totalRowsInserted, c.ErrorHelper.WrapIO(err,
						"failed to insert batch at rows %d-%d", startRowIdx, startRowIdx+currentBatchRows-1)
				}
				totalRowsInserted += rowsInserted

				// Start new batch with this row
				queryBuilder.Reset()
				queryBuilder.WriteString(baseQuery)
				queryBuilder.WriteString(rowString)
				currentBatchRows = 1
				startRowIdx = rowIdx
			} else {
				// Add row to current batch
				if currentBatchRows > 0 {
					queryBuilder.WriteString(",")
				}
				queryBuilder.WriteString(rowString)
				currentBatchRows++
			}
		}

		// Execute final batch for this record batch
		if currentBatchRows > 0 {
			rowsInserted, err := c.executeBatch(ctx, conn, queryBuilder.String(), params)
			if err != nil {
				return totalRowsInserted, c.ErrorHelper.WrapIO(err,
					"failed to insert final batch at rows %d-%d", startRowIdx, startRowIdx+currentBatchRows-1)
			}
			totalRowsInserted += rowsInserted
		}
	}

	if err := stream.Err(); err != nil {
		return totalRowsInserted, c.ErrorHelper.WrapIO(err, "stream error")
	}

	return totalRowsInserted, nil
}

// serializeSQLLiteral converts a Go value (produced by ConvertArrowToGo) to a
// SQL literal string.  This mirrors the presto go client's parameter
// interpolation so that the dynamic-batched ingest path produces the same
// literals as the parameterized path.
func serializeSQLLiteral(v any) (string, error) {
	switch val := v.(type) {
	case nil:
		return "NULL", nil
	case bool:
		if val {
			return "TRUE", nil
		}
		return "FALSE", nil
	case int:
		return strconv.FormatInt(int64(val), 10), nil
	case int8:
		return strconv.FormatInt(int64(val), 10), nil
	case int16:
		return strconv.FormatInt(int64(val), 10), nil
	case int32:
		return strconv.FormatInt(int64(val), 10), nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	case uint:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(val), 10), nil
	case uint64:
		return strconv.FormatUint(val, 10), nil
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'", nil
	case []byte:
		return "X'" + hex.EncodeToString(val) + "'", nil
	case time.Time:
		return "TIMESTAMP '" + val.Format("2006-01-02 15:04:05.000") + "'", nil
	default:
		return "", fmt.Errorf("unsupported value type for SQL serialization: %T", v)
	}
}

// getParameterPlaceholder returns the appropriate SQL placeholder for Arrow types that need casting.
//
// The presto go client interpolates parameters as plain literals, so types
// whose literal form does not match the target SQL type (e.g. decimals and
// times rendered as strings) are wrapped in CASTs.
func (c *prestoConnectionImpl) getParameterPlaceholder(field arrow.Field) string {
	if extName, exists := field.Metadata.GetValue("ARROW:extension:name"); exists && extName == "arrow.uuid" {
		return "CAST(? AS UUID)"
	}

	switch fieldType := field.Type.(type) {
	case *arrow.Int8Type:
		// PrestoDB does not narrow INTEGER literals in INSERT, so narrow
		// integer types need explicit CASTs.
		return "CAST(? AS TINYINT)"
	case *arrow.Int16Type:
		return "CAST(? AS SMALLINT)"
	case *arrow.Float32Type:
		return "CAST(? AS REAL)"
	case *arrow.Float64Type:
		return "CAST(? AS DOUBLE)"
	case arrow.DecimalType:
		return fmt.Sprintf("CAST(? AS DECIMAL(%d,%d))", fieldType.GetPrecision(), fieldType.GetScale())
	case *arrow.Time32Type, *arrow.Time64Type:
		return "CAST(? AS TIME)"
	case *arrow.Date32Type:
		return "CAST(? AS DATE)"
	case *arrow.TimestampType:
		if fieldType.TimeZone != "" {
			return "CAST(? AS TIMESTAMP WITH TIME ZONE)"
		}
		return "?"
	default:
		return "?"
	}
}

// buildRowString constructs a single row string like "(val1,val2,val3)" with CAST where needed.
// Returns the complete row string ready to be appended to the query.
func (c *prestoConnectionImpl) buildRowString(schema *arrow.Schema, serializedValues []string) string {
	var rowBuilder strings.Builder
	rowBuilder.WriteString("(")

	for colIdx, serializedValue := range serializedValues {
		if colIdx > 0 {
			rowBuilder.WriteString(",")
		}

		field := schema.Field(colIdx)

		// Get the placeholder (e.g., "?", "CAST(? AS REAL)", "CAST(? AS UUID)")
		// Then replace ? with the actual serialized value
		placeholder := c.getParameterPlaceholder(field)
		valueWithCast := strings.Replace(placeholder, "?", serializedValue, 1)
		rowBuilder.WriteString(valueWithCast)
	}

	rowBuilder.WriteString(")")
	return rowBuilder.String()
}

// executeBatch executes the current query batch and returns rows inserted.
func (c *prestoConnectionImpl) executeBatch(
	ctx context.Context,
	conn *sqlwrapper.LoggingConn,
	querySQL string,
	params []any,
) (int64, error) {
	if querySQL == "" {
		return 0, nil
	}

	result, err := conn.ExecContext(ctx, querySQL, params...)
	if err != nil {
		return 0, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

// createTableIfNeeded creates the table based on the ingest mode.
// qualifiedTable is an already-quoted, optionally catalog/schema-qualified identifier.
func (c *prestoConnectionImpl) createTableIfNeeded(ctx context.Context, conn *sqlwrapper.LoggingConn, qualifiedTable string, schema *arrow.Schema, options *driverbase.BulkIngestOptions, params []any) error {
	switch options.Mode {
	case adbc.OptionValueIngestModeCreate:
		// Create the table (fail if exists)
		return c.createTable(ctx, conn, qualifiedTable, schema, false, params)
	case adbc.OptionValueIngestModeCreateAppend:
		// Create the table if it doesn't exist
		return c.createTable(ctx, conn, qualifiedTable, schema, true, params)
	case adbc.OptionValueIngestModeReplace:
		// Drop and recreate the table
		if err := c.dropTable(ctx, conn, qualifiedTable, params); err != nil {
			return err
		}
		return c.createTable(ctx, conn, qualifiedTable, schema, false, params)
	case adbc.OptionValueIngestModeAppend:
		// Table should already exist, do nothing
		return nil
	default:
		return c.ErrorHelper.InvalidArgument("unsupported ingest mode: %s", options.Mode)
	}
}

// createTable creates a Presto table from Arrow schema.
// qualifiedTable is an already-quoted, optionally catalog/schema-qualified identifier.
func (c *prestoConnectionImpl) createTable(ctx context.Context, conn *sqlwrapper.LoggingConn, qualifiedTable string, schema *arrow.Schema, ifNotExists bool, params []any) error {
	var queryBuilder strings.Builder
	queryBuilder.WriteString("CREATE TABLE ")
	if ifNotExists {
		queryBuilder.WriteString("IF NOT EXISTS ")
	}
	queryBuilder.WriteString(qualifiedTable)
	queryBuilder.WriteString(" (")

	for i, field := range schema.Fields() {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}

		queryBuilder.WriteString(quoteIdentifier(field.Name))
		queryBuilder.WriteString(" ")

		// Convert Arrow type to Presto type
		prestoType, err := c.arrowToPrestoType(field)
		if err != nil {
			return c.ErrorHelper.WrapInternal(err, "convert Arrow type for column %s", field.Name)
		}
		queryBuilder.WriteString(prestoType)
	}

	queryBuilder.WriteString(")")

	_, err := conn.ExecContext(ctx, queryBuilder.String(), params...)
	return err
}

// dropTable drops a Presto table.
// qualifiedTable is an already-quoted, optionally catalog/schema-qualified identifier.
func (c *prestoConnectionImpl) dropTable(ctx context.Context, conn *sqlwrapper.LoggingConn, qualifiedTable string, params []any) error {
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", qualifiedTable)
	_, err := conn.ExecContext(ctx, dropSQL, params...)
	return err
}

// arrowToPrestoType converts Arrow data type to Presto column type.
//
// Unlike Trino, Presto does not support parameterized timestamp/time
// precision in DDL; TIMESTAMP and TIME always have millisecond precision.
func (c *prestoConnectionImpl) arrowToPrestoType(field arrow.Field) (string, error) {
	if extName, exists := field.Metadata.GetValue("ARROW:extension:name"); exists && extName == "arrow.uuid" {
		return "UUID", nil
	}

	var prestoType string

	switch arrowType := field.Type.(type) {
	case *arrow.BooleanType:
		prestoType = "BOOLEAN"
	case *arrow.Int8Type:
		prestoType = "TINYINT"
	case *arrow.Int16Type:
		prestoType = "SMALLINT"
	case *arrow.Int32Type:
		prestoType = "INTEGER"
	case *arrow.Int64Type:
		prestoType = "BIGINT"
	case *arrow.Float32Type:
		prestoType = "REAL"
	case *arrow.Float64Type:
		prestoType = "DOUBLE"
	case *arrow.StringType:
		prestoType = "VARCHAR"
	case *arrow.BinaryType:
		prestoType = "VARBINARY"
	case *arrow.BinaryViewType:
		prestoType = "VARBINARY"
	case *arrow.FixedSizeBinaryType:
		prestoType = "VARBINARY"
	case *arrow.LargeBinaryType:
		prestoType = "VARBINARY"
	case *arrow.Date32Type:
		prestoType = "DATE"
	case *arrow.TimestampType:
		// Presto timestamps have fixed millisecond precision; use TIMESTAMP
		// for timezone-naive values, TIMESTAMP WITH TIME ZONE for
		// timezone-aware values.  Sub-millisecond precision is truncated.
		if arrowType.TimeZone != "" {
			prestoType = "TIMESTAMP WITH TIME ZONE"
		} else {
			prestoType = "TIMESTAMP"
		}
	case *arrow.Time32Type, *arrow.Time64Type:
		// Presto times have fixed millisecond precision.
		prestoType = "TIME"
	case arrow.DecimalType:
		prestoType = fmt.Sprintf("DECIMAL(%d,%d)", arrowType.GetPrecision(), arrowType.GetScale())
	default:
		// Default to VARCHAR for unknown types
		prestoType = "VARCHAR"
	}

	// Note: In Presto, columns are nullable by default, and all columns are
	// assumed nullable since the presto go client does not provide
	// nullability metadata.
	return prestoType, nil
}

// ListTableTypes implements driverbase.TableTypeLister interface
func (c *prestoConnectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	// Presto supports these standard table types
	return []string{
		"BASE TABLE", // Regular tables
		"VIEW",       // Views
	}, nil
}

// quoteIdentifier properly quotes a SQL identifier, escaping any internal quotes
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// quoteIdentifiers quotes each identifier part and joins them with "." into a single
// qualified name such as "catalog"."schema"."table".
func quoteIdentifiers(parts []string) string {
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = quoteIdentifier(p)
	}
	return strings.Join(quoted, ".")
}

// qualifiedTableName builds a quoted, optionally catalog/schema-qualified
// table identifier such as "catalog"."schema"."table".
func qualifiedTableName(catalog, schema, table string) string {
	parts := make([]string, 0, 3)
	if catalog != "" {
		parts = append(parts, catalog)
	}
	if schema != "" {
		parts = append(parts, schema)
	}
	parts = append(parts, table)
	return quoteIdentifiers(parts)
}
