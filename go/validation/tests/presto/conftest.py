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

import os
import urllib.parse

import pytest


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        pytest.skip(f"{name} must be set for Presto URI validation tests")
    return value


def pytest_generate_tests(metafunc) -> None:
    metafunc.parametrize(
        "driver",
        [pytest.param("presto:", id="presto")],
        scope="module",
        indirect=["driver"],
    )


@pytest.fixture(scope="session")
def presto_host() -> str:
    """Presto host. Example: PRESTO_HOST=localhost"""
    return require_env("PRESTO_HOST")


@pytest.fixture(scope="session")
def presto_port() -> str:
    """Presto port. Example: PRESTO_PORT=8080"""
    return require_env("PRESTO_PORT")


@pytest.fixture(scope="session")
def presto_http_port(presto_port: str, presto_ssl_mode: str) -> str:
    """Plain HTTP port exposed by the local Presto Docker setup."""
    if presto_ssl_mode == "http":
        return presto_port
    return os.environ.get("PRESTO_HTTP_PORT", "8080")


@pytest.fixture(scope="session")
def presto_https_port(presto_port: str, presto_ssl_mode: str) -> str:
    """HTTPS port exposed by the local Presto Docker setup."""
    if presto_ssl_mode == "https":
        return presto_port
    return os.environ.get("PRESTO_HTTPS_PORT", "8443")


@pytest.fixture(scope="session")
def presto_catalog() -> str:
    """Presto catalog name. Example: PRESTO_CATALOG=memory"""
    return require_env("PRESTO_CATALOG")


@pytest.fixture(scope="session")
def presto_schema() -> str:
    """Presto schema name. Example: PRESTO_SCHEMA=default"""
    return require_env("PRESTO_SCHEMA")


@pytest.fixture(scope="session")
def presto_ssl_mode() -> str:
    """Presto transport mode. Example: PRESTO_SSL_MODE=https"""
    return require_env("PRESTO_SSL_MODE").lower()


@pytest.fixture(scope="session")
def presto_ssl_cert_path() -> str:
    """CA cert path for self-signed HTTPS. Example: PRESTO_SSL_CERT_PATH=/path/to/ca.crt"""
    return require_env("PRESTO_SSL_CERT_PATH")


@pytest.fixture(scope="session")
def presto_username() -> str:
    """Presto username. Example: PRESTO_USERNAME=test"""
    return require_env("PRESTO_USERNAME")


@pytest.fixture(scope="session")
def presto_uri_query(presto_ssl_mode: str, presto_ssl_cert_path: str) -> str:
    """Connection options appended to `presto://` URIs."""
    query_params: list[tuple[str, str]] = []
    if presto_ssl_mode == "https":
        query_params.append(("ssl_ca", presto_ssl_cert_path))

    return urllib.parse.urlencode(query_params)


@pytest.fixture(scope="session")
def presto_http_scheme(presto_ssl_mode: str) -> str:
    return "https" if presto_ssl_mode == "https" else "http"


@pytest.fixture(scope="session")
def uri(
    presto_host: str,
    presto_port: str,
    presto_catalog: str,
    presto_schema: str,
    presto_uri_query: str,
) -> str:
    """
    Constructs a clean Presto URI without credentials.
    Example: presto://localhost:8080/memory/default?SSL=false
    """
    return f"presto://{presto_host}:{presto_port}/{presto_catalog}/{presto_schema}?{presto_uri_query}"


@pytest.fixture(scope="session")
def dsn(
    presto_username: str,
    presto_host: str,
    presto_port: str,
    presto_catalog: str,
    presto_schema: str,
    presto_http_scheme: str,
    presto_ssl_cert_path: str,
) -> str:
    """
    Constructs an HTTP(S)-style connection URI.
    Example: http://test@localhost:8080/memory/default
    """
    query_params: list[tuple[str, str]] = []
    if presto_http_scheme == "https":
        query_params.append(("ssl_ca", presto_ssl_cert_path))

    query = urllib.parse.urlencode(query_params)
    suffix = f"?{query}" if query else ""
    return (
        f"{presto_http_scheme}://{presto_username}@{presto_host}:{presto_port}"
        f"/{presto_catalog}/{presto_schema}{suffix}"
    )
