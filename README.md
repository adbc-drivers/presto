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

# ADBC Driver for Presto

Not affiliated with Presto.

An [ADBC driver](https://arrow.apache.org/adbc/) for
[Presto](https://prestodb.io/), built on the
[presto-go-client](https://github.com/prestodb/presto-go-client).

## Installation

Pre-packaged builds are not yet published.  Once available from the
[Columnar](https://columnar.tech) CDN, they can be installed by any tool that
supports [ADBC](https://arrow.apache.org/adbc/) Driver Manifests, such as
[dbc](https://columnar.tech/dbc):

```sh
dbc install presto
```

See [Building](#building) if you would rather build the drivers yourself.

## Usage

The driver accepts the following URI forms via the `uri` database option:

- `presto://user:pass@host:8080/catalog/schema` — native form; unrecognized
  query parameters become Presto session properties
- `http://host:8080/catalog/schema` or `https://host:8443/catalog/schema`
- `host:8080` — bare host and port (HTTP)

TLS is configured with the `ssl_ca`, `ssl_cert`, `ssl_key`, and
`ssl_skip_verify` query parameters, or implied by an `https://` URI.

## Building

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
