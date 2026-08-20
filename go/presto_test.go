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

package presto_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/adbc-drivers/driverbase-go/testutil"
	"github.com/adbc-drivers/driverbase-go/validation"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	presto "github.com/adbc-drivers/presto"
)

// PrestoQuirks implements validation.DriverQuirks for Presto ADBC driver
type PrestoQuirks struct {
	dsn string
	mem *memory.CheckedAllocator
}

func (q *PrestoQuirks) SetupDriver(t *testing.T) driverbase.DriverWithContext {
	q.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	return presto.NewDriver(q.mem)
}

func (q *PrestoQuirks) TearDownDriver(t *testing.T, _ driverbase.DriverWithContext) {
	q.mem.AssertSize(t, 0)
}

func (q *PrestoQuirks) DatabaseOptions() map[string]string {
	return map[string]string{
		adbc.OptionKeyURI: q.dsn,
	}
}

func (q *PrestoQuirks) CreateSampleTable(tableName string, r arrow.RecordBatch) error {
	// Use standard database/sql to create table directly
	db, err := sql.Open("presto", q.dsn)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	// Drop table if it exists first to ensure clean state
	_, err = db.Exec("DROP TABLE IF EXISTS " + tableName)
	if err != nil {
		return fmt.Errorf("failed to drop existing table: %w", err)
	}

	// Build CREATE TABLE statement based on Arrow schema
	var createQuery strings.Builder
	createQuery.WriteString("CREATE TABLE ")
	createQuery.WriteString(tableName)
	createQuery.WriteString(" (")

	schema := r.Schema()
	for i, field := range schema.Fields() {
		if i > 0 {
			createQuery.WriteString(", ")
		}
		createQuery.WriteString(field.Name)
		createQuery.WriteString(" ")

		// Map Arrow types to Presto types
		switch field.Type.ID() {
		case arrow.INT32:
			createQuery.WriteString("INTEGER")
		case arrow.INT64:
			createQuery.WriteString("BIGINT")
		case arrow.STRING:
			createQuery.WriteString("VARCHAR(255)")
		case arrow.FLOAT32:
			createQuery.WriteString("REAL")
		case arrow.FLOAT64:
			createQuery.WriteString("DOUBLE")
		case arrow.BOOL:
			createQuery.WriteString("BOOLEAN")
		default:
			createQuery.WriteString("VARCHAR") // Default fallback
		}

		// Note: Presto's memory connector does not support NOT NULL columns,
		// so nullability is not encoded in DDL here.
	}
	createQuery.WriteString(")")

	_, err = db.Exec(createQuery.String())
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Insert data from Arrow record
	if r.NumRows() > 0 {
		// Insert each row separately to handle NULL values correctly
		for row := range r.NumRows() {
			var insertQuery strings.Builder
			insertQuery.WriteString("INSERT INTO ")
			insertQuery.WriteString(tableName)
			insertQuery.WriteString(" VALUES (")

			values := make([]any, r.NumCols())
			for col := range r.NumCols() {
				column := r.Column(int(col))
				if column.IsNull(int(row)) {
					values[col] = nil
				} else {
					// Extract value based on column type
					switch arr := column.(type) {
					case *array.Int32:
						values[col] = arr.Value(int(row))
					case *array.Int64:
						values[col] = arr.Value(int(row))
					case *array.String:
						values[col] = arr.Value(int(row))
					case *array.Float32:
						values[col] = arr.Value(int(row))
					case *array.Float64:
						values[col] = arr.Value(int(row))
					case *array.Boolean:
						values[col] = arr.Value(int(row))
					default:
						values[col] = fmt.Sprintf("%v", column)
					}
				}
			}

			// Build placeholders and collect non-null values for prepared statement
			var queryParams []any
			for i, val := range values {
				if i > 0 {
					insertQuery.WriteString(", ")
				}
				if val == nil {
					insertQuery.WriteString("NULL")
				} else {
					// PrestoDB does not narrow literals, so narrow types
					// need explicit CASTs around the interpolated value.
					switch schema.Field(i).Type.ID() {
					case arrow.FLOAT32:
						insertQuery.WriteString("CAST(? AS REAL)")
					default:
						insertQuery.WriteString("?")
					}
					queryParams = append(queryParams, val)
				}
			}
			insertQuery.WriteString(")")

			_, err = db.Exec(insertQuery.String(), queryParams...)
			if err != nil {
				return fmt.Errorf("failed to insert row %d: %w", row, err)
			}
		}
	}

	return nil
}

func (q *PrestoQuirks) DropTable(cnxn adbc.ConnectionWithContext, tblName string) error {
	stmt, err := cnxn.NewStatement(context.Background())
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, stmt.Close(context.Background()))
	}()

	if err = stmt.SetSqlQuery(context.Background(), "DROP TABLE IF EXISTS "+tblName); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(context.Background())
	return err
}

func (q *PrestoQuirks) SampleTableSchemaMetadata(tblName string, dt arrow.DataType) arrow.Metadata {
	// Return metadata that matches what our Presto type converter actually returns
	metadata := map[string]string{}

	switch dt.ID() {
	case arrow.INT32:
		metadata["sql.column_name"] = "ints"
		metadata["sql.database_type_name"] = "INTEGER"
	case arrow.INT64:
		metadata["sql.column_name"] = "ints"
		metadata["sql.database_type_name"] = "BIGINT"
	case arrow.STRING:
		metadata["sql.column_name"] = "strings"
		metadata["sql.database_type_name"] = "VARCHAR"
	case arrow.FLOAT32:
		metadata["sql.column_name"] = "floats"
		metadata["sql.database_type_name"] = "REAL"
	case arrow.FLOAT64:
		metadata["sql.column_name"] = "doubles"
		metadata["sql.database_type_name"] = "DOUBLE"
	case arrow.BOOL:
		metadata["sql.column_name"] = "bools"
		metadata["sql.database_type_name"] = "BOOLEAN"
	}

	return arrow.MetadataFrom(metadata)
}

func (q *PrestoQuirks) Alloc() memory.Allocator      { return q.mem }
func (q *PrestoQuirks) BindParameter(idx int) string { return "?" }

// TODO(https://github.com/adbc-drivers/driverbase-go/issues/126)
func (q *PrestoQuirks) SupportsBulkIngest(string) bool              { return false }
func (q *PrestoQuirks) SupportsConcurrentStatements() bool          { return false }
func (q *PrestoQuirks) SupportsCurrentCatalogSchema() bool          { return true }
func (q *PrestoQuirks) SupportsExecuteSchema() bool                 { return false }
func (q *PrestoQuirks) SupportsGetSetOptions() bool                 { return true }
func (q *PrestoQuirks) SupportsPartitionedData() bool               { return false }
func (q *PrestoQuirks) SupportsStatistics() bool                    { return true }
func (q *PrestoQuirks) SupportsTransactions() bool                  { return false }
func (q *PrestoQuirks) SupportsGetParameterSchema() bool            { return false }
func (q *PrestoQuirks) SupportsDynamicParameterBinding() bool       { return false }
func (q *PrestoQuirks) SupportsErrorIngestIncompatibleSchema() bool { return false }
func (q *PrestoQuirks) SupportsGetTableSchema() bool                { return true }
func (q *PrestoQuirks) Catalog() string                             { return "memory" }
func (q *PrestoQuirks) DBSchema() string                            { return "default" }

func (q *PrestoQuirks) GetMetadata(code adbc.InfoCode) any {
	switch code {
	case adbc.InfoDriverName:
		return "ADBC Driver Foundry Driver for Presto"
	case adbc.InfoDriverVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverArrowVersion:
		return "v18.7.0"
	case adbc.InfoVendorVersion:
		return regexp.MustCompile(`Presto [0-9]+`)
	case adbc.InfoVendorArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverADBCVersion:
		return adbc.AdbcVersion1_1_0
	case adbc.InfoVendorName:
		return "Presto"
	case adbc.InfoVendorSql:
		return true
	case adbc.InfoVendorSubstrait:
		return false
	}
	return nil
}

func withQuirks(t *testing.T, fn func(*PrestoQuirks)) {
	dsn := os.Getenv("PRESTO_DSN")
	if dsn == "" {
		t.Skip("Set PRESTO_DSN environment variable for validation tests")
	}

	q := &PrestoQuirks{dsn: dsn}
	fn(q)
}

type PrestoStatementTests struct {
	validation.StatementTests
}

func TestGetStatisticsIncludesTable(t *testing.T) {
	withQuirks(t, func(q *PrestoQuirks) {
		ctx := context.Background()

		drv := q.SetupDriver(t)
		defer q.TearDownDriver(t, drv)

		db, err := drv.NewDatabaseWithContext(ctx, q.DatabaseOptions())
		require.NoError(t, err)
		defer func() {
			require.NoError(t, db.Close(ctx))
		}()

		cnxn, err := db.Open(ctx)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, cnxn.Close(ctx))
		}()

		stats, ok := cnxn.(adbc.ConnectionGetStatistics)
		require.True(t, ok)

		// Note: Presto's memory connector does not report statistics, so use
		// the built-in tpch catalog which has known table statistics.
		cat := "tpch"
		sch := "tiny"
		tableName := "nation"
		rdr, err := stats.GetStatistics(ctx, &cat, &sch, &tableName, true)
		require.NoError(t, err)
		defer rdr.Release()

		require.True(t, adbc.GetStatisticsSchema.Equal(rdr.Schema()))

		var tableStats []testutil.Statistic
		for rdr.Next() {
			rec := rdr.RecordBatch()
			stats := testutil.ExtractStatisticsForTable(rec, cat, sch, tableName)
			if len(stats) > 0 {
				tableStats = append(tableStats, stats...)
			}
		}
		require.NoError(t, rdr.Err())
		require.NotEmpty(t, tableStats, "expected statistics output to include table %s.%s.%s", cat, sch, tableName)

		// Convert to lookup map for easier access
		statsMap := testutil.StatisticsToLookupMap(tableStats)

		// Validate row count: tpch.tiny.nation always has 25 rows
		rowCount, ok := statsMap[int16(adbc.StatisticRowCountKey)].StatisticValue.(float64)
		require.True(t, ok, "expected row count statistic as float64")
		require.InDelta(t, 25.0, rowCount, 0.1, "expected row count to be approximately 25")

		// Validate a column-level statistic: nationkey has 25 distinct values
		for _, stat := range tableStats {
			if stat.ColumnName != nil && *stat.ColumnName == "nationkey" && stat.StatisticKey == int16(adbc.StatisticDistinctCountKey) {
				ndv, ok := stat.StatisticValue.(float64)
				require.True(t, ok, "expected distinct count to be float64")
				require.InDelta(t, 25.0, ndv, 0.1, "expected distinct count for 'nationkey' to be approximately 25")
			}
		}
	})
}

func TestGetStatisticsWithWildcardCatalog(t *testing.T) {
	withQuirks(t, func(q *PrestoQuirks) {
		ctx := context.Background()

		drv := q.SetupDriver(t)
		defer q.TearDownDriver(t, drv)

		db, err := drv.NewDatabaseWithContext(ctx, q.DatabaseOptions())
		require.NoError(t, err)
		defer func() {
			require.NoError(t, db.Close(ctx))
		}()

		cnxn, err := db.Open(ctx)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, cnxn.Close(ctx))
		}()

		stats, ok := cnxn.(adbc.ConnectionGetStatistics)
		require.True(t, ok)

		// Test with wildcard catalog pattern (should use system.jdbc.tables)
		cat := "tpch"
		wildcardCat := "tpc%"
		sch := "tiny"
		tableName := "nation"
		rdr, err := stats.GetStatistics(ctx, &wildcardCat, &sch, &tableName, true)
		require.NoError(t, err)
		defer rdr.Release()

		require.True(t, adbc.GetStatisticsSchema.Equal(rdr.Schema()))

		var tableStats []testutil.Statistic
		for rdr.Next() {
			rec := rdr.RecordBatch()
			stats := testutil.ExtractStatisticsForTable(rec, cat, sch, tableName)
			if len(stats) > 0 {
				tableStats = append(tableStats, stats...)
			}
		}
		require.NoError(t, rdr.Err())
		require.NotEmpty(t, tableStats, "expected wildcard catalog query to find table %s.%s.%s", cat, sch, tableName)

		// Convert to lookup map for easier access
		statsMap := testutil.StatisticsToLookupMap(tableStats)

		// Validate at least row count exists
		rowCount, ok := statsMap[int16(adbc.StatisticRowCountKey)].StatisticValue.(float64)
		require.True(t, ok, "expected row count statistic as float64 from wildcard query")
		require.InDelta(t, 25.0, rowCount, 0.1, "expected row count to be approximately 25")
	})
}

func (s *PrestoStatementTests) TestSqlIngestErrors() {
	s.T().Skip()
}

// TestValidation runs the comprehensive ADBC validation test suite
// This is the primary test that validates ADBC specification compliance
func TestValidation(t *testing.T) {
	withQuirks(t, func(q *PrestoQuirks) {
		suite.Run(t, &validation.DatabaseTests{Quirks: q})
		suite.Run(t, &validation.ConnectionTests{Quirks: q})
		suite.Run(t, &validation.StatementTests{Quirks: q})
	})
}

// -------------------- Additional Tests --------------------

type PrestoTests struct {
	suite.Suite

	Quirks *PrestoQuirks

	ctx    context.Context
	driver driverbase.DriverWithContext
	db     adbc.DatabaseWithContext
	cnxn   adbc.ConnectionWithContext
	stmt   adbc.StatementWithContext
}

func (s *PrestoTests) SetupTest() {
	var err error
	s.ctx = context.Background()
	s.driver = s.Quirks.SetupDriver(s.T())
	s.db, err = s.driver.NewDatabaseWithContext(s.ctx, s.Quirks.DatabaseOptions())
	s.NoError(err)
	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)
	s.stmt, err = s.cnxn.NewStatement(s.ctx)
	s.NoError(err)
}

func (s *PrestoTests) TearDownTest() {
	s.NoError(s.stmt.Close(s.ctx))
	s.NoError(s.cnxn.Close(s.ctx))
	s.Quirks.TearDownDriver(s.T(), s.driver)
	s.cnxn = nil
	s.NoError(s.db.Close(s.ctx))
	s.db = nil
	s.driver = nil
}

type selectCase struct {
	name     string
	query    string
	schema   *arrow.Schema
	expected string
}

func (s *PrestoTests) TestIngestQueryId() {
	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     "ints",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
	}, nil)
	batch := testutil.RecordFromJSON(s.T(), s.Quirks.Alloc(), schema, `[{"ints": 1}, {"ints": 2}, {"ints": 3}]`)
	defer batch.Release()
	s.Require().NoError(s.stmt.Bind(s.ctx, batch))
	s.Require().NoError(s.stmt.SetOption(s.ctx, adbc.OptionKeyIngestTargetTable, "foobar"))
	s.Require().NoError(s.stmt.SetOption(s.ctx, adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeReplace))

	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
}

func (s *PrestoTests) TestSelect() {
	// Drop table if it exists first, then create test table with basic Presto types
	s.NoError(s.stmt.SetSqlQuery(s.ctx, `DROP TABLE IF EXISTS memory.default.test_types`))
	_, err := s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	s.NoError(s.stmt.SetSqlQuery(s.ctx, `
		CREATE TABLE memory.default.test_types (
			bool_col BOOLEAN,
			tinyint_col TINYINT,
			int_col INTEGER,
			bigint_col BIGINT,
			float_col REAL,
			double_col DOUBLE,
			varchar_col VARCHAR(100)
		)
	`))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	// Insert multiple rows including NULLs to test nullable behavior.
	// PrestoDB does not narrow INTEGER/DOUBLE literals, so narrow types
	// need typed literals.
	s.NoError(s.stmt.SetSqlQuery(s.ctx, `
		INSERT INTO memory.default.test_types VALUES
			(true, TINYINT '42', 12345, BIGINT '9876543210', REAL '3.25', 6.75, 'hello world'),
			(false, NULL, 54321, NULL, REAL '1.5', NULL, NULL),
			(true, TINYINT '100', 99999, BIGINT '1234567890', REAL '2.0', 8.5, 'test string')
	`))
	_, err = s.stmt.ExecuteUpdate(s.ctx)
	s.NoError(err)

	for _, testCase := range []selectCase{
		{
			name:  "boolean",
			query: "SELECT bool_col AS istrue FROM memory.default.test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "istrue",
					Type:     arrow.FixedWidthTypes.Boolean,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "istrue",
						"sql.database_type_name": "BOOLEAN",
					}),
				},
			}, nil),
			expected: `[{"istrue": true}, {"istrue": false}, {"istrue": true}]`,
		},
		{
			name:  "tinyint",
			query: "SELECT tinyint_col AS value FROM memory.default.test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Int8,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "TINYINT",
					}),
				},
			}, nil),
			expected: `[{"value": 42}, {"value": null}, {"value": 100}]`,
		},
		{
			name:  "int32",
			query: "SELECT int_col AS theanswer FROM memory.default.test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "theanswer",
						"sql.database_type_name": "INTEGER",
					}),
				},
			}, nil),
			expected: `[{"theanswer": 12345}, {"theanswer": 54321}, {"theanswer": 99999}]`,
		},
		{
			name:  "int64",
			query: "SELECT bigint_col AS theanswer FROM memory.default.test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "theanswer",
					Type:     arrow.PrimitiveTypes.Int64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "theanswer",
						"sql.database_type_name": "BIGINT",
					}),
				},
			}, nil),
			expected: `[{"theanswer": 9876543210}, {"theanswer": null}, {"theanswer": 1234567890}]`,
		},
		{
			name:  "float32",
			query: "SELECT float_col AS value FROM memory.default.test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Float32,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "REAL",
					}),
				},
			}, nil),
			expected: `[{"value": 3.25}, {"value": 1.5}, {"value": 2.0}]`,
		},
		{
			name:  "float64",
			query: "SELECT double_col AS value FROM memory.default.test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "value",
					Type:     arrow.PrimitiveTypes.Float64,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "value",
						"sql.database_type_name": "DOUBLE",
					}),
				},
			}, nil),
			expected: `[{"value": 6.75}, {"value": null}, {"value": 8.5}]`,
		},
		{
			name:  "string",
			query: "SELECT varchar_col AS greeting FROM memory.default.test_types",
			schema: arrow.NewSchema([]arrow.Field{
				{
					Name:     "greeting",
					Type:     arrow.BinaryTypes.String,
					Nullable: true,
					Metadata: arrow.MetadataFrom(map[string]string{
						"sql.column_name":        "greeting",
						"sql.database_type_name": "VARCHAR",
					}),
				},
			}, nil),
			expected: `[{"greeting": "hello world"}, {"greeting": null}, {"greeting": "test string"}]`,
		},
	} {
		s.Run(testCase.name, func() {
			s.NoError(s.stmt.SetSqlQuery(s.ctx, testCase.query))

			rdr, rows, err := s.stmt.ExecuteQuery(s.ctx)
			s.NoError(err)
			if rdr != nil {
				defer rdr.Release()
			}

			s.Truef(testCase.schema.Equal(rdr.Schema()), "expected: %s\ngot: %s", testCase.schema, rdr.Schema())
			s.Equal(int64(-1), rows)
			s.Truef(rdr.Next(), "no record, error? %s", rdr.Err())

			expectedRecord, _, err := array.RecordFromJSON(s.Quirks.Alloc(), testCase.schema, bytes.NewReader([]byte(testCase.expected)))
			s.NoError(err)
			defer expectedRecord.Release()

			rec := rdr.RecordBatch()
			s.NotNil(rec)

			s.Truef(array.RecordEqual(expectedRecord, rec), "expected: %s\ngot: %s", expectedRecord, rec)

			s.False(rdr.Next())
			s.NoError(rdr.Err())
		})
	}
}

type PrestoTestSuite struct {
	suite.Suite
	dsn    string
	mem    *memory.CheckedAllocator
	ctx    context.Context
	driver driverbase.DriverWithContext
	db     adbc.DatabaseWithContext
	cnxn   adbc.ConnectionWithContext
	stmt   adbc.StatementWithContext
}

func (s *PrestoTestSuite) SetupSuite() {
	var err error
	s.dsn = os.Getenv("PRESTO_DSN")
	if s.dsn == "" {
		s.T().Skip("Set PRESTO_DSN environment variable")
	}

	s.ctx = context.Background()
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)

	s.driver = presto.NewDriver(s.mem)
	s.db, err = s.driver.NewDatabaseWithContext(s.ctx, map[string]string{
		adbc.OptionKeyURI: s.dsn,
	})
	s.NoError(err)

	s.cnxn, err = s.db.Open(s.ctx)
	s.NoError(err)

	s.stmt, err = s.cnxn.NewStatement(s.ctx)
	s.NoError(err)
}

func (s *PrestoTestSuite) TearDownSuite() {
	if s.stmt != nil {
		s.NoError(s.stmt.Close(s.ctx))
	}
	if s.cnxn != nil {
		s.NoError(s.cnxn.Close(s.ctx))
	}
	if s.db != nil {
		s.NoError(s.db.Close(s.ctx))
	}
	s.mem.AssertSize(s.T(), 0)
}

func TestPrestoTypeTests(t *testing.T) {
	dsn := os.Getenv("PRESTO_DSN")
	if dsn == "" {
		t.Skip("Set PRESTO_DSN environment variable for type tests")
	}

	quirks := &PrestoQuirks{dsn: dsn}
	suite.Run(t, &PrestoTests{Quirks: quirks})
}

func TestPrestoIntegrationSuite(t *testing.T) {
	suite.Run(t, new(PrestoTestSuite))
}

// TestURIParsing tests BuildPrestoDSN with various URI formats
func TestURIParsing(t *testing.T) {
	factory := presto.NewPrestoDBFactory()

	tests := []struct {
		name          string
		prestoURI     string
		username      string
		password      string
		expectedDSN   string
		shouldError   bool
		errorContains string
	}{
		{
			name:        "basic presto with port and catalog/schema",
			prestoURI:   "presto://user:pass@localhost:8080/hive/default",
			expectedDSN: "presto://user:pass@localhost:8080/hive/default",
		},
		{
			name:        "presto without port defaults to 8080",
			prestoURI:   "presto://user:pass@localhost/memory/default",
			expectedDSN: "presto://user:pass@localhost:8080/memory/default",
		},
		{
			name:        "presto without catalog/schema",
			prestoURI:   "presto://user:pass@localhost:8080",
			expectedDSN: "presto://user:pass@localhost:8080",
		},
		{
			name:        "presto with only catalog, no schema",
			prestoURI:   "presto://user:pass@localhost:8080/postgresql",
			expectedDSN: "presto://user:pass@localhost:8080/postgresql",
		},
		{
			name:        "presto with custom port",
			prestoURI:   "presto://user:pass@example.com:9999/hive/sales",
			expectedDSN: "presto://user:pass@example.com:9999/hive/sales",
		},
		{
			name:        "presto with ip address",
			prestoURI:   "presto://user:pass@127.0.0.1:8080/memory/test",
			expectedDSN: "presto://user:pass@127.0.0.1:8080/memory/test",
		},
		{
			name:        "presto with ipv6 host",
			prestoURI:   "presto://user:pass@[::1]:8080/hive/default",
			expectedDSN: "presto://user:pass@[::1]:8080/hive/default",
		},
		{
			name:        "presto with ipv6 host, default port",
			prestoURI:   "presto://user:pass@[::1]/memory/default",
			expectedDSN: "presto://user:pass@[::1]:8080/memory/default",
		},
		{
			name:        "no credentials in uri",
			prestoURI:   "presto://localhost:8080/hive/default",
			expectedDSN: "presto://localhost:8080/hive/default",
		},
		{
			name:        "only username in uri",
			prestoURI:   "presto://user@localhost:8080/memory/default",
			expectedDSN: "presto://user@localhost:8080/memory/default",
		},
		{
			name:        "override credentials with options",
			prestoURI:   "presto://olduser:oldpass@localhost:8080/hive/default",
			username:    "newuser",
			password:    "newpass",
			expectedDSN: "presto://newuser:newpass@localhost:8080/hive/default",
		},
		{
			name:        "add credentials via options",
			prestoURI:   "presto://localhost:8080/memory/default",
			username:    "admin",
			password:    "secret",
			expectedDSN: "presto://admin:secret@localhost:8080/memory/default",
		},
		{
			name:        "override only username",
			prestoURI:   "presto://user:pass@localhost:8080/hive/default",
			username:    "newuser",
			expectedDSN: "presto://newuser:pass@localhost:8080/hive/default",
		},
		{
			name:        "override only password",
			prestoURI:   "presto://user:pass@localhost:8080/hive/default",
			password:    "newpass",
			expectedDSN: "presto://user:newpass@localhost:8080/hive/default",
		},
		{
			name:        "single query parameter",
			prestoURI:   "presto://user:pass@localhost:8080/hive/default?source=myapp",
			expectedDSN: "presto://user:pass@localhost:8080/hive/default?source=myapp",
		},
		{
			name:        "session properties pass through",
			prestoURI:   "presto://user:pass@localhost:8080/hive/default?query_max_stage_count=100&source=myapp",
			expectedDSN: "presto://user:pass@localhost:8080/hive/default?query_max_stage_count=100&source=myapp",
		},
		{
			name:        "credentials with special characters",
			prestoURI:   "presto://my%40user:p%40ssword@localhost:8080/hive/default",
			expectedDSN: "presto://my%40user:p%40ssword@localhost:8080/hive/default",
		},
		{
			name:        "ssl_ca implies https and default port 8443",
			prestoURI:   "presto://user@localhost/hive/default?ssl_ca=%2Fpath%2Fca.pem",
			expectedDSN: "presto://user@localhost:8443/hive/default?ssl_skip_verify=true",
		},
		{
			name:        "ssl_skip_verify implies https",
			prestoURI:   "presto://user@localhost:8443/hive/default?ssl_skip_verify=true",
			expectedDSN: "presto://user@localhost:8443/hive/default?ssl_skip_verify=true",
		},
		{
			name:        "bare host defaults to presto scheme and port 8080",
			prestoURI:   "localhost:8080",
			expectedDSN: "presto://localhost:8080",
		},
		{
			name:        "bare host without port",
			prestoURI:   "localhost",
			expectedDSN: "presto://localhost:8080",
		},
		{
			name:        "http uri converts to presto",
			prestoURI:   "http://user:pass@localhost:8080/hive/default",
			expectedDSN: "presto://user:pass@localhost:8080/hive/default",
		},
		{
			name:        "https uri converts to presto with TLS",
			prestoURI:   "https://user:pass@localhost/hive/default",
			expectedDSN: "presto://user:pass@localhost:8443/hive/default?ssl_skip_verify=true",
		},
		{
			name:        "https uri keeps explicit port",
			prestoURI:   "https://user:pass@localhost:9443/hive/default",
			expectedDSN: "presto://user:pass@localhost:9443/hive/default?ssl_skip_verify=true",
		},
		{
			name:        "add credentials via options (http format)",
			prestoURI:   "http://localhost:8080/memory/default?source=myapp",
			username:    "admin",
			password:    "secret",
			expectedDSN: "presto://admin:secret@localhost:8080/memory/default?source=myapp",
		},
		{
			name:          "invalid presto uri format",
			prestoURI:     "presto://[invalid-uri",
			shouldError:   true,
			errorContains: "invalid URI format",
		},
		{
			name:          "unsupported scheme",
			prestoURI:     "ftp://localhost:8080",
			shouldError:   true,
			errorContains: "unsupported URI scheme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := map[string]string{
				adbc.OptionKeyURI: tt.prestoURI,
			}
			if tt.username != "" {
				opts[adbc.OptionKeyUsername] = tt.username
			}
			if tt.password != "" {
				opts[adbc.OptionKeyPassword] = tt.password
			}

			result, err := factory.BuildPrestoDSN(opts)

			if tt.shouldError {
				require.ErrorContains(t, err, tt.errorContains)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.expectedDSN, result)
		})
	}
}
