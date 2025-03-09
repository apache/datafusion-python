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

from pathlib import Path

import pytest
from datafusion import SessionContext


@pytest.fixture
def ctx():
    return SessionContext()


def test_read_parquet(ctx):
    ctx.register_parquet(
        "test",
        f"file://{Path.cwd()}/parquet/data/alltypes_plain.parquet",
        table_partition_cols=[],
        parquet_pruning=True,
        file_extension=".parquet",
    )
    df = ctx.sql("SELECT * FROM test")
    assert isinstance(df.collect(), list)
