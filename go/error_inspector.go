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
	"errors"
	"net/http"

	"github.com/apache/arrow-adbc/go/adbc"
	presto "github.com/prestodb/presto-go-client/v2"
)

type PrestoErrorInspector struct{}

// InspectError examines a Presto error and formats it as an ADBC error.
// Presto error code mapping for reference:
// https://github.com/prestodb/presto/blob/master/presto-spi/src/main/java/com/facebook/presto/spi/StandardErrorCode.java
//
// Note: unlike some engines, Presto's REST API does not return SQLSTATE
// values, so the SqlState field is always empty.
func (t PrestoErrorInspector) InspectError(err error, defaultStatus adbc.Status) adbc.Error {
	status := defaultStatus
	var vendorCode int32
	var sqlState [5]byte

	if queryErr, ok := errors.AsType[*presto.QueryError](err); ok {
		vendorCode = int32(queryErr.ErrorCode)

		switch queryErr.ErrorType {
		case "USER_ERROR":
			// User errors include syntax errors, invalid arguments, etc.
			// Check error name for more specific mapping
			switch queryErr.ErrorName {
			case "SYNTAX_ERROR", "INVALID_COLUMN_REFERENCE", "MISSING_COLUMN_NAME", "DUPLICATE_COLUMN_NAME":
				status = adbc.StatusInvalidArgument
			case "NOT_FOUND", "CATALOG_NOT_FOUND", "COLUMN_NOT_FOUND", "TABLE_NOT_FOUND", "SCHEMA_NOT_FOUND", "FUNCTION_NOT_FOUND", "MISSING_CATALOG_NAME", "MISSING_SCHEMA_NAME":
				status = adbc.StatusNotFound
			case "ALREADY_EXISTS":
				status = adbc.StatusAlreadyExists
			case "PERMISSION_DENIED":
				status = adbc.StatusUnauthorized
			case "NOT_SUPPORTED":
				status = adbc.StatusNotImplemented
			case "INVALID_CAST_ARGUMENT", "INVALID_FUNCTION_ARGUMENT":
				status = adbc.StatusInvalidArgument
			case "NUMERIC_VALUE_OUT_OF_RANGE", "DIVISION_BY_ZERO":
				status = adbc.StatusInvalidData
			case "CONSTRAINT_VIOLATION":
				status = adbc.StatusIntegrity
			case "USER_CANCELED":
				status = adbc.StatusCancelled
			case "ABANDONED_QUERY", "EXCEEDED_TIME_LIMIT":
				status = adbc.StatusTimeout
			default:
				status = adbc.StatusInvalidArgument
			}

		case "INTERNAL_ERROR":
			status = adbc.StatusInternal

		case "EXTERNAL":
			status = adbc.StatusUnknown

		case "INSUFFICIENT_RESOURCES":
			status = adbc.StatusInternal
		}

		// If status still not determined, use official Presto error code
		// ranges as fallback (StandardErrorCode bases).
		if status == defaultStatus {
			switch {
			case queryErr.ErrorCode >= 0 && queryErr.ErrorCode < 0x0001_0000:
				// USER_ERROR range
				status = adbc.StatusInvalidArgument
			case queryErr.ErrorCode >= 0x0001_0000 && queryErr.ErrorCode < 0x0002_0000:
				// INTERNAL_ERROR range
				status = adbc.StatusInternal
			case queryErr.ErrorCode >= 0x0002_0000 && queryErr.ErrorCode < 0x0100_0000:
				// INSUFFICIENT_RESOURCES range
				status = adbc.StatusInternal
			case queryErr.ErrorCode >= 0x0100_0000:
				// EXTERNAL range (connector-specific codes)
				status = adbc.StatusUnknown
			}
		}
	} else if httpErr, ok := errors.AsType[*presto.ErrorResponse](err); ok {
		// HTTP-level errors (authentication, routing, etc.)
		if httpErr.Response != nil {
			switch httpErr.Response.StatusCode {
			case http.StatusUnauthorized:
				status = adbc.StatusUnauthenticated
			case http.StatusForbidden:
				status = adbc.StatusUnauthorized
			case http.StatusNotFound:
				status = adbc.StatusNotFound
			case http.StatusServiceUnavailable, http.StatusTooManyRequests:
				status = adbc.StatusIO
			default:
				status = adbc.StatusIO
			}
		}
	}

	return adbc.Error{
		Code:       status,
		Msg:        err.Error(),
		VendorCode: vendorCode,
		SqlState:   sqlState,
	}
}
