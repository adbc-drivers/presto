---
# Copyright (c) 2025 ADBC Drivers Contributors
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
{}
---

{{ cross_reference|safe }}
# Presto Driver {{ version }}

{{ heading|safe }}

This driver provides access to [Presto][presto], a free and
open-source distributed SQL query engine.

## Installation

The Presto driver can be installed with [dbc](https://docs.columnar.tech/dbc):

```bash
dbc install presto
```

## Connecting

To use the driver, provide a Presto connection string as the `uri` option.

```python
from adbc_driver_manager import dbapi

dbapi.connect(
  driver="presto",
  db_kwargs={
      "uri": "presto://user@localhost:8080/tpch/tiny"
  }
)
```

Note: The example above is for Python using the [adbc-driver-manager](https://pypi.org/project/adbc-driver-manager) package but the process will be similar for other driver managers.  See [adbc-quickstarts](https://github.com/columnar-tech/adbc-quickstarts).

### Connection String Format

```
presto://[user[:password]@]host[:port][/catalog[/schema]][?attribute1=value1&attribute2=value2...]
```

Components:
- Scheme: `presto://` (also accepts `http://` and `https://`)
- `user`: Optional (for authentication)
- `password`: Optional (for authentication, requires user)
- `host`: Required (no default)
- `port`: Optional (defaults to 8080 for HTTP, 8443 for HTTPS)
- `catalog`: Optional (Presto catalog name)
- `schema`: Optional (schema within catalog)
- Query params: TLS attributes (see below); unrecognized parameters are
  passed to Presto as session properties

:::{note}
Reserved characters in URI elements must be URI-encoded. For example, `@` becomes `%40`. If you include a zone ID in an IPv6 address, the `%` character used as the separator must be replaced with `%25`.
:::

#### HTTPS/SSL Configuration

By default, connections use HTTP.  HTTPS is used when the URI scheme is
`https://` or when any of the following query parameters is present:

- `ssl_ca`: Path to a PEM CA certificate used to verify the server
- `ssl_cert` and `ssl_key`: Paths to a PEM client certificate and key for
  mutual TLS
- `ssl_skip_verify=true`: Disable server certificate verification while
  maintaining an encrypted HTTPS connection (for self-signed certificates;
  not recommended for production)

Examples:

- `presto://localhost:8080/hive/default` → HTTP on port 8080
- `https://presto.example.com/hive/sales` → HTTPS on default port 8443,
  verified against the system trust store
- `presto://presto.example.com/hive/sales?ssl_ca=/path/to/ca.pem` → HTTPS
  with a custom CA
- `presto://user@localhost:8443/hive/default?ssl_skip_verify=true` → HTTPS
  without certificate verification (self-signed certificates)
- `presto://user@localhost:8080/memory/default?query_max_stage_count=100` →
  HTTP with a Presto session property

See [Presto Concepts](https://prestodb.io/docs/current/overview/concepts.html#catalog) for more information on catalogs and schemas, and the [Go Presto Client documentation](https://github.com/prestodb/presto-go-client#readme) for the underlying DSN reference.

## Feature & Type Support

{{ features|safe }}

### Types

{{ types|safe }}

## Compatibility

{{ compatibility_info|safe }}

{{ footnotes|safe }}

[presto]: https://prestodb.io/
