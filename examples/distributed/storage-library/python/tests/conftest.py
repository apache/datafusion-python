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
def readings_dir(tmp_path: pathlib.Path) -> pathlib.Path:
    """Three Parquet files, so the provider reports three partitions.

    Written as `part-0/1/2` rather than in one file because the file *is* the
    partition for this provider, and a single-file table would hide every
    partition-routing mistake.
    """
    directory = tmp_path / "readings"
    directory.mkdir()
    for index in range(3):
        base = index * 100
        pq.write_table(
            pa.table(
                {
                    "sensor_id": [base, base + 1, base + 2],
                    "reading": [1.5, 2.5, 3.5],
                }
            ),
            directory / f"part-{index}.parquet",
        )
    return directory
