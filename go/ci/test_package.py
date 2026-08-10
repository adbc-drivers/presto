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

import adbc_driver_manager.dbapi
import pytest


def test_package() -> None:
    uri = "presto://localhost:1234?ssl_ca=missing-test-ca.pem"
    with pytest.raises(
        adbc_driver_manager.dbapi.ProgrammingError,
        match="failed to read CA certificate",
    ):
        with adbc_driver_manager.dbapi.connect(
            driver="presto", uri=uri, autocommit=True
        ):
            pass
