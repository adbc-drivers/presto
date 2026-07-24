<!--
  Copyright (c) 2025 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Validation Suite Setup

1. Start the Docker container:

   ```shell
   docker compose up --detach --wait
   ```
2. Set the environment variable:

   ```shell
   export PRESTO_HOST="localhost"
   export PRESTO_PORT="8080"
   export PRESTO_CATALOG="memory"
   export PRESTO_SCHEMA="default"
   export PRESTO_SSL_MODE="http"
   export PRESTO_USERNAME="test"
   export PRESTO_DSN="presto://test@localhost:8080/memory/default"
   ```

   The local Docker setup serves plain HTTP only.  When validating against an
   HTTPS deployment, switch `PRESTO_SSL_MODE` to `https`, set
   `PRESTO_SSL_CERT_PATH` to the CA certificate, and use:

   ```shell
   export PRESTO_DSN="presto://test@host:8443/memory/default?ssl_ca=/path/to/ca.crt"
   ```

   `PRESTO_DSN` is used by the general validation suite. The URI-focused tests in
   `validation/tests/presto/test_uri.py` build their own connection strings from
   the `PRESTO_HOST`, `PRESTO_PORT`, `PRESTO_CATALOG`, `PRESTO_SCHEMA`,
   `PRESTO_SSL_MODE`, `PRESTO_SSL_CERT_PATH`, and `PRESTO_USERNAME` variables.
3. Run the tests:

   ```shell
   cd validation
   pixi run test
   ```
