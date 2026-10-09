# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Generator
    from typing import Any


class _FailOnWarning(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno >= logging.WARNING:
            err = f"Unexpected log warning from '{record.name}': {self.format(record)}"
            raise AssertionError(err)


@pytest.fixture(autouse=True)
def fail_on_log_warnings() -> Generator[None, Any, None]:
    handler = _FailOnWarning()
    logging.root.addHandler(handler)
    yield
    logging.root.removeHandler(handler)


@pytest.fixture
def lineitem(tmp_path: pathlib.Path) -> pathlib.Path:
    """A TPC-H-shaped slice, small enough to check the arithmetic by hand.

    Two files, because the aggregate has to be correct when DataFusion splits
    it into a partial pass per partition and merges the results.
    """
    directory = tmp_path / "lineitem"
    directory.mkdir()
    pq.write_table(
        pa.table(
            {
                "l_extendedprice": [100.0, 200.0],
                "l_discount": [0.0, 0.5],
                "l_tax": [0.0, 0.0],
                "l_quantity": [1.0, 3.0],
            }
        ),
        directory / "part-0.parquet",
    )
    pq.write_table(
        pa.table(
            {
                "l_extendedprice": [400.0],
                "l_discount": [0.25],
                "l_tax": [0.1],
                "l_quantity": [4.0],
            }
        ),
        directory / "part-1.parquet",
    )
    return directory
