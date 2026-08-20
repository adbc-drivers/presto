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

import urllib.parse

import adbc_driver_manager.dbapi
import pytest
from adbc_drivers_validation import model


def get_session_property(cursor, name: str):
    """Fetch a session property value via SHOW SESSION."""
    cursor.execute("SHOW SESSION")
    for row in cursor.fetchall():
        if row[0] == name:
            return row[1]
    return None


def test_username_uri(
    driver: model.DriverQuirks,
    driver_path: str,
    uri: str,  # presto://localhost:8080/memory/default
    presto_username: str,
) -> None:
    """Test authentication with username embedded in URI."""

    parsed = urllib.parse.urlparse(uri)
    query_params = urllib.parse.parse_qs(parsed.query)
    # Unrecognized query parameters become Presto session properties.
    query_params["task_concurrency"] = ["2"]

    new_query = urllib.parse.urlencode(query_params, doseq=True)
    netloc = f"{presto_username}@{parsed.netloc}"

    auth_uri = urllib.parse.urlunparse(
        (parsed.scheme, netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": auth_uri},
    ) as conn:
        assert conn.adbc_current_catalog == "memory"
        assert conn.adbc_current_db_schema == "default"

        with conn.cursor() as cursor:
            assert get_session_property(cursor, "task_concurrency") == "2"


def test_username_options(
    driver: model.DriverQuirks,
    driver_path: str,
    uri: str,
    presto_username: str,
) -> None:
    """Test authentication with username in connection options."""
    params = {
        "uri": uri,
        "username": presto_username,
    }
    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs=params,
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT 1")


@pytest.mark.parametrize("ssl_mode", ["trusted_ca", "skip_verification", "plain_http"])
def test_ssl_modes(
    driver: model.DriverQuirks,
    driver_path: str,
    presto_host: str,
    presto_catalog: str,
    presto_schema: str,
    presto_username: str,
    presto_http_port: str,
    presto_https_port: str,
    presto_ssl_cert_path: str,
    ssl_mode: str,
) -> None:
    """Test trusted HTTPS, HTTPS with disabled verification, and plain HTTP."""
    port = presto_https_port
    query_params: list[tuple[str, str]] = []

    if ssl_mode == "trusted_ca":
        query_params.append(("ssl_ca", presto_ssl_cert_path))
    elif ssl_mode == "skip_verification":
        query_params.append(("ssl_skip_verify", "true"))
    elif ssl_mode == "plain_http":
        port = presto_http_port
    else:
        raise AssertionError(f"unexpected ssl_mode {ssl_mode}")

    ssl_uri = urllib.parse.urlunparse(
        (
            "presto",
            f"{presto_username}@{presto_host}:{port}",
            f"/{presto_catalog}/{presto_schema}",
            "",
            urllib.parse.urlencode(query_params),
            "",
        )
    )

    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": ssl_uri},
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        assert result[0] == 1


def test_uri_catalog_schema_parsing(
    driver: model.DriverQuirks,
    driver_path: str,
    presto_host: str,
    presto_port: str,
    presto_username: str,
    presto_uri_query: str,
) -> None:
    """Tests that catalog and schema are correctly parsed from URI path."""

    full_uri = (
        f"presto://{presto_username}@{presto_host}:{presto_port}"
        f"/memory/test_schema?{presto_uri_query}"
    )

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": full_uri},
    ) as conn:
        assert conn.adbc_current_catalog == "memory"
        assert conn.adbc_current_db_schema == "test_schema"


def test_uri_catalog_only(
    driver: model.DriverQuirks,
    driver_path: str,
    presto_host: str,
    presto_port: str,
    presto_username: str,
    presto_uri_query: str,
) -> None:
    """Tests URI with catalog but no schema."""

    catalog_only_uri = f"presto://{presto_username}@{presto_host}:{presto_port}/memory?{presto_uri_query}"

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": catalog_only_uri},
    ) as conn:
        assert conn.adbc_current_catalog == "memory"


def test_ipv6_host_support(
    driver: model.DriverQuirks,
    driver_path: str,
    presto_username: str,
    presto_port: str,
    presto_catalog: str,
    presto_schema: str,
    presto_uri_query: str,
    presto_ssl_mode: str,
) -> None:
    """Tests that IPv6 addresses are correctly handled in URIs."""
    if presto_ssl_mode == "https":
        pytest.skip("local HTTPS cert covers localhost/127.0.0.1, not ::1")

    ipv6_uri = (
        f"presto://{presto_username}@[::1]:{presto_port}/{presto_catalog}/{presto_schema}"
        f"?{presto_uri_query}"
    )

    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": ipv6_uri},
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1


def test_url_encoded_catalog_schema(
    driver: model.DriverQuirks,
    driver_path: str,
    presto_host: str,
    presto_port: str,
    presto_username: str,
    presto_uri_query: str,
) -> None:
    """Tests that URL-encoded catalog and schema names work correctly."""

    encoded_uri = (
        f"presto://{presto_username}@{presto_host}:{presto_port}"
        f"/my%20catalog/my%20schema?{presto_uri_query}"
    )

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": encoded_uri},
    ) as conn:
        assert conn.adbc_current_catalog == "my catalog"
        assert conn.adbc_current_db_schema == "my schema"


def test_missing_uri_raises_error(
    driver: model.DriverQuirks,
    driver_path: str,
) -> None:
    """Tests that connecting without a 'uri' option raises an error."""
    with (
        pytest.raises(
            adbc_driver_manager.dbapi.ProgrammingError,
            match="missing required option uri",
        ),
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={},
        ),
    ):
        pass


def test_invalid_uri_format(
    driver: model.DriverQuirks,
    driver_path: str,
) -> None:
    """Tests that a malformed URI raises a helpful error."""
    with (
        pytest.raises(
            adbc_driver_manager.dbapi.ProgrammingError,
            match="invalid URI format",
        ),
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={"uri": "presto://[invalid-format"},
        ),
    ):
        pass


# --- HTTP(S)-style URI tests ---


def test_basic_dsn_connection(
    driver: model.DriverQuirks,
    driver_path: str,
    dsn: str,  # Example: http://test@localhost:8080/memory/default
) -> None:
    """
    Test basic connection using the HTTP(S) URI form, adding extra parameters
    to ensure all query args are preserved.
    """

    parsed = urllib.parse.urlparse(dsn)

    query_params = urllib.parse.parse_qs(parsed.query)
    # Unrecognized query parameters become Presto session properties.
    query_params["task_concurrency"] = ["2"]

    new_query = urllib.parse.urlencode(query_params, doseq=True)

    modified_dsn = urllib.parse.urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment,
        )
    )

    with adbc_driver_manager.dbapi.connect(
        driver=driver_path,
        db_kwargs={"uri": modified_dsn},
    ) as conn:
        assert conn.adbc_current_catalog == "memory"
        assert conn.adbc_current_db_schema == "default"

        with conn.cursor() as cursor:
            assert get_session_property(cursor, "task_concurrency") == "2"


def test_plain_host_with_username_options(
    driver: model.DriverQuirks,
    driver_path: str,
    presto_username: str,
    presto_host: str,
    presto_port: str,
    presto_ssl_mode: str,
    presto_ssl_cert_path: str,
) -> None:
    """
    Tests that a plain host string
    is correctly combined with credentials from options.
    """
    query_params: list[tuple[str, str]] = []
    if presto_ssl_mode == "https":
        query_params.append(("ssl_ca", presto_ssl_cert_path))

    query = urllib.parse.urlencode(query_params)
    suffix = f"?{query}" if query else ""

    with (
        adbc_driver_manager.dbapi.connect(
            driver=driver_path,
            db_kwargs={
                "uri": f"{presto_host}:{presto_port}{suffix}",
                "username": presto_username,
            },
        ) as conn,
        conn.cursor() as cursor,
    ):
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1
