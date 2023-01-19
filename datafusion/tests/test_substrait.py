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

import os

import pyarrow as pa
import pyarrow.dataset as ds

from datafusion import column, literal, SessionContext
import pytest


@pytest.fixture
def config():
    return Config()

def test_create_context_no_args():
    SessionContext()


def test_register_table(ctx, database):
    default = ctx.catalog()
    public = default.database("public")
    assert public.names() == {"csv", "csv1", "csv2"}
    table = public.table("csv")

    ctx.register_table("csv3", table)
    assert public.names() == {"csv", "csv1", "csv2", "csv3"}


def test_register_dataset(ctx):
    # create a RecordBatch and register it as a pyarrow.dataset.Dataset
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    dataset = ds.dataset([batch])
    ctx.register_dataset("t", dataset)

    assert ctx.tables() == {"t"}

    result = ctx.sql("SELECT a+b, a-b FROM t").collect()

    assert result[0].column(0) == pa.array([5, 7, 9])
    assert result[0].column(1) == pa.array([-3, -3, -3])

