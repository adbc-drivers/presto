#!/bin/sh

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

# Generate the self-signed CA certificate and PKCS#12 keystore used by the
# local Presto Docker Compose HTTPS setup.

set -eu

CERT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
CA_CONFIG="${CERT_DIR}/openssl-ca.cnf"
SERVER_CONFIG="${CERT_DIR}/openssl-server.cnf"

CA_CERT="${CERT_DIR}/ca.crt"
KEYSTORE="${CERT_DIR}/localhost.p12"

if [ -f "${CA_CERT}" ] && [ -f "${KEYSTORE}" ] && [ "${PRESTO_FORCE_REGENERATE_CERTS:-0}" != "1" ]; then
  echo "Presto TLS assets already exist"
  exit 0
fi

if [ ! -f "${CA_CONFIG}" ] || [ ! -f "${SERVER_CONFIG}" ]; then
  echo "Missing OpenSSL config under ${CERT_DIR}" >&2
  exit 1
fi

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/presto-certs.XXXXXX")
cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT INT TERM

run_openssl() {
  log_file="${TMP_DIR}/openssl.log"
  if ! openssl "$@" > /dev/null 2>"${log_file}"; then
    cat "${log_file}" >&2
    exit 1
  fi
}

CA_KEY="${TMP_DIR}/ca.key"
SERVER_KEY="${TMP_DIR}/localhost.key"
SERVER_CSR="${TMP_DIR}/localhost.csr"
CA_SERIAL="${TMP_DIR}/ca.srl"
TMP_CA_CERT="${TMP_DIR}/ca.crt"
TMP_SERVER_CERT="${TMP_DIR}/localhost.crt"
TMP_KEYSTORE="${TMP_DIR}/localhost.p12"

run_openssl req \
  -x509 \
  -newkey rsa:2048 \
  -days 3650 \
  -nodes \
  -keyout "${CA_KEY}" \
  -out "${TMP_CA_CERT}" \
  -config "${CA_CONFIG}"

run_openssl req \
  -new \
  -newkey rsa:2048 \
  -nodes \
  -keyout "${SERVER_KEY}" \
  -out "${SERVER_CSR}" \
  -config "${SERVER_CONFIG}"

run_openssl x509 \
  -req \
  -days 3650 \
  -in "${SERVER_CSR}" \
  -CA "${TMP_CA_CERT}" \
  -CAkey "${CA_KEY}" \
  -CAcreateserial \
  -CAserial "${CA_SERIAL}" \
  -out "${TMP_SERVER_CERT}" \
  -extfile "${SERVER_CONFIG}" \
  -extensions v3_req

run_openssl pkcs12 \
  -export \
  -out "${TMP_KEYSTORE}" \
  -inkey "${SERVER_KEY}" \
  -in "${TMP_SERVER_CERT}" \
  -certfile "${TMP_CA_CERT}" \
  -name presto-dev \
  -passout pass:presto-dev

cp "${TMP_CA_CERT}" "${CA_CERT}"
cp "${TMP_KEYSTORE}" "${KEYSTORE}"
chmod 0644 "${CA_CERT}" "${KEYSTORE}"

echo "Generated Presto TLS assets in ${CERT_DIR}"
