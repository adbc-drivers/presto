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
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/adbc-drivers/driverbase-go/driverbase"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type prestoTableRef struct {
	catalog string
	schema  string
	table   string
}

// GetStatistics returns table and column statistics.
//
// Presto statistics are always approximate (marked statistic_is_approximate=true).
// The 'approximate' parameter controls error handling:
//   - true:  skip tables that error (best-effort)
//   - false: fail on any error (strict)
func (c *prestoConnectionImpl) GetStatistics(ctx context.Context, catalog, dbSchema, tableName *string, approximate bool) (array.RecordReader, error) {
	// ADBC semantics: empty string means "only objects without this property".
	// Presto always has catalog/schema/table names, so these filters produce no results.
	if (catalog != nil && *catalog == "") || (dbSchema != nil && *dbSchema == "") || (tableName != nil && *tableName == "") {
		return driverbase.EmptyGetStatisticsReader()
	}

	tables, err := c.getStatisticsTables(ctx, catalog, dbSchema, tableName)
	if err != nil {
		return nil, err
	}

	statsByCatalog := map[string]map[string][]driverbase.Statistic{}
	var catalogOrder []string
	schemaOrder := map[string][]string{}
	seenCatalog := map[string]bool{}
	seenSchema := map[string]map[string]bool{}

	for _, tbl := range tables {
		addCatalogSchemaToMaps(tbl.catalog, tbl.schema, seenCatalog, &catalogOrder, statsByCatalog, schemaOrder, seenSchema)

		stats, err := c.getTableStatistics(ctx, tbl, approximate)
		if err != nil {
			// When approximate is requested, be best-effort and skip tables that
			// error (connector/table may not support statistics).
			if approximate {
				continue
			}
			return nil, err
		}

		if len(stats) == 0 {
			continue
		}
		statsByCatalog[tbl.catalog][tbl.schema] = append(statsByCatalog[tbl.catalog][tbl.schema], stats...)
	}

	return driverbase.BuildGetStatisticsReader(c.Alloc, catalogOrder, schemaOrder, statsByCatalog)
}

// addCatalogSchemaToMaps tracks catalog and schema in the statistics collection maps,
// maintaining insertion order and initializing nested structures as needed.
func addCatalogSchemaToMaps(
	cat, sch string,
	seenCatalog map[string]bool,
	catalogOrder *[]string,
	statsByCatalog map[string]map[string][]driverbase.Statistic,
	schemaOrder map[string][]string,
	seenSchema map[string]map[string]bool,
) {
	if !seenCatalog[cat] {
		seenCatalog[cat] = true
		*catalogOrder = append(*catalogOrder, cat)
		statsByCatalog[cat] = map[string][]driverbase.Statistic{}
		schemaOrder[cat] = nil
	}
	if seenSchema[cat] == nil {
		seenSchema[cat] = map[string]bool{}
	}
	if !seenSchema[cat][sch] {
		seenSchema[cat][sch] = true
		schemaOrder[cat] = append(schemaOrder[cat], sch)
	}
}

func (c *prestoConnectionImpl) GetStatisticNames(ctx context.Context) (array.RecordReader, error) {
	// Presto has no custom statistics (uses only standard ADBC statistics)
	return driverbase.BuildGetStatisticNamesReader(c.Alloc, nil)
}

func (c *prestoConnectionImpl) getStatisticsTables(ctx context.Context, catalog, dbSchema, tableName *string) (tables []prestoTableRef, err error) {
	var queryBuilder strings.Builder

	// Use the appropriate metadata source based on catalog filter:
	// - If catalog is a literal (no wildcards), qualify information_schema with it
	// - Otherwise, use system.jdbc.tables which provides cross-catalog access
	useCatalogQualified := catalog != nil && !strings.ContainsAny(*catalog, "%_")

	if useCatalogQualified {
		// Query specific catalog's information_schema
		fmt.Fprintf(&queryBuilder, `
			SELECT table_catalog, table_schema, table_name
			FROM %s.information_schema.tables
			WHERE table_type = 'BASE TABLE'`, quoteIdentifier(*catalog))
	} else {
		// Query system catalog for cross-catalog access
		queryBuilder.WriteString(`
			SELECT table_cat, table_schem, table_name
			FROM system.jdbc.tables
			WHERE table_type = 'TABLE'`)
	}

	args := []any{}
	if catalog != nil {
		if useCatalogQualified {
			// Already qualified, no filter needed
		} else {
			// Apply LIKE filter for patterns or when catalog is nil
			queryBuilder.WriteString(` AND table_cat LIKE ?`)
			args = append(args, *catalog)
		}
	}
	if dbSchema != nil {
		if useCatalogQualified {
			queryBuilder.WriteString(` AND table_schema LIKE ?`)
		} else {
			queryBuilder.WriteString(` AND table_schem LIKE ?`)
		}
		args = append(args, *dbSchema)
	}
	if tableName != nil {
		queryBuilder.WriteString(` AND table_name LIKE ?`)
		args = append(args, *tableName)
	}

	if useCatalogQualified {
		queryBuilder.WriteString(` ORDER BY table_catalog, table_schema, table_name`)
	} else {
		queryBuilder.WriteString(` ORDER BY table_cat, table_schem, table_name`)
	}

	rows, err := c.Conn.QueryContext(ctx, queryBuilder.String(), args...)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to query tables for statistics")
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	for rows.Next() {
		var cat, sch, tbl string
		if err := rows.Scan(&cat, &sch, &tbl); err != nil {
			return nil, c.ErrorHelper.WrapIO(err, "failed to scan table for statistics")
		}
		tables = append(tables, prestoTableRef{catalog: cat, schema: sch, table: tbl})
	}
	if err := rows.Err(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "error during table iteration for statistics")
	}

	return tables, nil
}

func (c *prestoConnectionImpl) getTableStatistics(ctx context.Context, tbl prestoTableRef, approximate bool) (stats []driverbase.Statistic, err error) {
	qualified := fmt.Sprintf("%s.%s.%s", quoteIdentifier(tbl.catalog), quoteIdentifier(tbl.schema), quoteIdentifier(tbl.table))
	query := "SHOW STATS FOR " + qualified

	rows, err := c.Conn.QueryContext(ctx, query)
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to query stats for %s.%s.%s", tbl.catalog, tbl.schema, tbl.table)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	var tableRowCount float64
	var hasTableRowCount bool

	// PrestoDB's SHOW STATS returns 7 core columns (column_name, data_size,
	// distinct_values_count, nulls_fraction, row_count, low_value,
	// high_value); newer versions append additional columns (e.g.
	// histogram).  Scan the core columns and discard the rest.
	cols, err := rows.Columns()
	if err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "failed to get stats columns for %s.%s.%s", tbl.catalog, tbl.schema, tbl.table)
	}
	if len(cols) < 7 {
		return nil, c.ErrorHelper.Internal("unexpected SHOW STATS column count %d for %s.%s.%s", len(cols), tbl.catalog, tbl.schema, tbl.table)
	}

	type colRow struct {
		columnName sql.NullString
		dataSize   sql.NullFloat64
		ndv        sql.NullFloat64 // Number of Distinct Values
		nullFrac   sql.NullFloat64
		rowCount   sql.NullFloat64
		low        sql.NullString
		high       sql.NullString
	}
	var colRows []colRow

	for rows.Next() {
		var r colRow
		dest := make([]any, len(cols))
		dest[0], dest[1], dest[2], dest[3], dest[4], dest[5], dest[6] =
			&r.columnName, &r.dataSize, &r.ndv, &r.nullFrac, &r.rowCount, &r.low, &r.high
		for i := 7; i < len(cols); i++ {
			dest[i] = new(sql.RawBytes)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, c.ErrorHelper.WrapIO(err, "failed to scan stats row for %s.%s.%s", tbl.catalog, tbl.schema, tbl.table)
		}

		if !r.columnName.Valid {
			if r.rowCount.Valid {
				tableRowCount = r.rowCount.Float64
				hasTableRowCount = true
			}
		} else {
			colRows = append(colRows, r)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, c.ErrorHelper.WrapIO(err, "error during stats iteration for %s.%s.%s", tbl.catalog, tbl.schema, tbl.table)
	}

	// Presto statistics are always approximate (sampling-based, HyperLogLog, etc.).
	// Always mark statistic_is_approximate=true regardless of the 'approximate' parameter.
	isApprox := true
	_ = approximate // Ignored for marking statistics; only used for error handling (see GetStatistics)

	if hasTableRowCount {
		stats = append(stats, driverbase.NewFloat64Stat(tbl.table, nil, int16(adbc.StatisticRowCountKey), tableRowCount, isApprox))
	}

	for _, r := range colRows {
		colName := r.columnName.String

		if r.ndv.Valid {
			stats = append(stats, driverbase.NewFloat64Stat(tbl.table, &colName, int16(adbc.StatisticDistinctCountKey), r.ndv.Float64, isApprox))
		}

		if hasTableRowCount && r.nullFrac.Valid {
			frac := r.nullFrac.Float64
			if !math.IsNaN(frac) {
				nullCount := frac * tableRowCount
				stats = append(stats, driverbase.NewFloat64Stat(tbl.table, &colName, int16(adbc.StatisticNullCountKey), nullCount, isApprox))
			}
		}

		if r.low.Valid {
			stats = append(stats, driverbase.NewBinaryStat(tbl.table, &colName, int16(adbc.StatisticMinValueKey), []byte(r.low.String), isApprox))
		}

		if r.high.Valid {
			stats = append(stats, driverbase.NewBinaryStat(tbl.table, &colName, int16(adbc.StatisticMaxValueKey), []byte(r.high.String), isApprox))
		}

		// Presto SHOW STATS data_size is an estimated total size for the column.
		// Map this to ADBC average byte width by dividing by the (estimated) row count.
		if hasTableRowCount && tableRowCount > 0 && r.dataSize.Valid {
			avgByteWidth := r.dataSize.Float64 / tableRowCount
			stats = append(stats, driverbase.NewFloat64Stat(tbl.table, &colName, int16(adbc.StatisticAverageByteWidthKey), avgByteWidth, isApprox))
		}
	}

	return stats, nil
}
