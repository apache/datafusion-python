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
from dfx_engine.session import SessionSpec

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


# One row group per file, four files, TPC-H `lineitem` column names. Small
# enough that every expected value below is checked by hand, and partitioned
# so there is something to distribute -- the real SF-1 dataset is a single
# 220 MB file per table, which would give one partition and no fan-out. See
# `run_tpch.py` for the same queries against the real thing.
ROW_FIELDS = "returnflag, linestatus, quantity, extendedprice, discount, tax"
_ROWS = [
    ("A", "F", 1.0, 100.0, 0.00, 0.00),
    ("N", "O", 2.0, 200.0, 0.10, 0.00),
    ("A", "F", 3.0, 300.0, 0.00, 0.10),
    ("R", "F", 4.0, 400.0, 0.20, 0.00),
    ("N", "O", 5.0, 500.0, 0.00, 0.00),
    ("A", "F", 6.0, 600.0, 0.50, 0.00),
    ("R", "F", 7.0, 700.0, 0.00, 0.20),
    ("N", "O", 8.0, 800.0, 0.25, 0.00),
]


@pytest.fixture
def lineitem_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """`lineitem` as four Parquet files, two rows each."""
    directory = tmp_path / "lineitem"
    directory.mkdir()
    for index in range(4):
        chunk = _ROWS[index * 2 : index * 2 + 2]
        pq.write_table(
            pa.table(
                {
                    "l_returnflag": [row[0] for row in chunk],
                    "l_linestatus": [row[1] for row in chunk],
                    "l_quantity": [row[2] for row in chunk],
                    "l_extendedprice": [row[3] for row in chunk],
                    "l_discount": [row[4] for row in chunk],
                    "l_tax": [row[5] for row in chunk],
                }
            ),
            directory / f"part-{index}.parquet",
        )
    return directory


@pytest.fixture
def spec(lineitem_dir: pathlib.Path, tmp_path: pathlib.Path) -> SessionSpec:
    """A distributed spec: four input partitions, a fresh shuffle directory."""
    return SessionSpec(
        tables={"lineitem": str(lineitem_dir)},
        shuffle_dir=str(tmp_path / "shuffle"),
        target_partitions=2,
    )
