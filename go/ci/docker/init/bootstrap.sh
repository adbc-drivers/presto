#!/usr/bin/env sh
# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Initialize extra catalogs/schemas for the validation suite.
#
# Runs statements through the Presto REST API (requires curl and jq).  A
# Presto query only executes as the client follows nextUri, so each statement
# is polled to completion.

set -eu

PRESTO_SERVER="${PRESTO_SERVER:-http://presto:8080}"

run_statement() {
    stmt="$1"
    echo "presto-init: ${stmt}"
    resp=$(curl -sf -X POST -H "X-Presto-User: init" --data "${stmt}" "${PRESTO_SERVER}/v1/statement")
    next=$(echo "${resp}" | jq -r '.nextUri // empty')
    while [ -n "${next}" ]; do
        resp=$(curl -sf "${next}")
        err=$(echo "${resp}" | jq -r '.error.message // empty')
        if [ -n "${err}" ]; then
            echo "presto-init: statement failed: ${err}" >&2
            exit 1
        fi
        next=$(echo "${resp}" | jq -r '.nextUri // empty')
        if [ -n "${next}" ]; then
            sleep 0.2
        fi
    done
}

run_statement "CREATE SCHEMA IF NOT EXISTS secondmemory.myschema"
run_statement "CREATE SCHEMA IF NOT EXISTS memory.secondschema"
