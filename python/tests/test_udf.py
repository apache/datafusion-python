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

import uuid

import pyarrow as pa
import pytest
from datafusion import column, udf

UUID_EXTENSION_AVAILABLE = hasattr(pa, "uuid")


@pytest.fixture
def df(ctx):
    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 4, None])],
        names=["a", "b"],
    )
    return ctx.create_dataframe([[batch]], name="test_table")


def test_udf(df):
    # is_null is a pa function over arrays
    is_null = udf(
        lambda x: x.is_null(),
        [pa.int64()],
        pa.bool_(),
        volatility="immutable",
    )

    df = df.select(is_null(column("b")))
    result = df.collect()[0].column(0)

    assert result == pa.array([False, False, True])


def test_udf_decorator(df):
    @udf([pa.int64()], pa.bool_(), "immutable")
    def is_null(x: pa.Array) -> pa.Array:
        return x.is_null()

    df = df.select(is_null(column("b")))
    result = df.collect()[0].column(0)
    assert result == pa.array([False, False, True])


def test_register_udf(ctx, df) -> None:
    is_null = udf(
        lambda x: x.is_null(),
        [pa.float64()],
        pa.bool_(),
        volatility="immutable",
        name="is_null",
    )

    ctx.register_udf(is_null)

    df_result = ctx.sql("select is_null(b) from test_table")
    result = df_result.collect()[0].column(0)

    assert result == pa.array([False, False, True])


class OverThresholdUDF:
    def __init__(self, threshold: int = 0) -> None:
        self.threshold = threshold

    def __call__(self, values: pa.Array) -> pa.Array:
        return pa.array(v.as_py() >= self.threshold for v in values)


def test_udf_with_parameters_function(df) -> None:
    udf_no_param = udf(
        OverThresholdUDF(),
        pa.int64(),
        pa.bool_(),
        volatility="immutable",
    )

    df1 = df.select(udf_no_param(column("a")))
    result = df1.collect()[0].column(0)

    assert result == pa.array([True, True, True])

    udf_with_param = udf(
        OverThresholdUDF(2),
        pa.int64(),
        pa.bool_(),
        volatility="immutable",
    )

    df2 = df.select(udf_with_param(column("a")))
    result = df2.collect()[0].column(0)

    assert result == pa.array([False, True, True])


def test_udf_with_parameters_decorator(df) -> None:
    @udf([pa.int64()], pa.bool_(), "immutable")
    def udf_no_param(values: pa.Array) -> pa.Array:
        return OverThresholdUDF()(values)

    df1 = df.select(udf_no_param(column("a")))
    result = df1.collect()[0].column(0)

    assert result == pa.array([True, True, True])

    @udf([pa.int64()], pa.bool_(), "immutable")
    def udf_with_param(values: pa.Array) -> pa.Array:
        return OverThresholdUDF(2)(values)

    df2 = df.select(udf_with_param(column("a")))
    result = df2.collect()[0].column(0)

    assert result == pa.array([False, True, True])


@pytest.mark.skipif(
    not UUID_EXTENSION_AVAILABLE,
    reason="PyArrow uuid extension helper unavailable",
)
def test_uuid_extension_chain(ctx) -> None:
    uuid_type = pa.uuid()
    uuid_field = pa.field("uuid_col", uuid_type)

    first = udf(
        lambda values: values,
        [uuid_field],
        uuid_field,
        volatility="immutable",
        name="uuid_identity",
    )

    def ensure_extension(values: pa.Array | pa.ChunkedArray) -> pa.Array:
        if isinstance(values, pa.ChunkedArray):
            assert values.type.equals(uuid_type)
            return values.combine_chunks()
        assert isinstance(values, pa.ExtensionArray)
        assert values.type.equals(uuid_type)
        return values

    second = udf(
        ensure_extension,
        [uuid_field],
        uuid_field,
        volatility="immutable",
        name="uuid_assert",
    )

    # The UUID extension metadata should survive UDF registration.
    assert getattr(uuid_type, "extension_name", None) == "arrow.uuid"
    assert getattr(uuid_field.type, "extension_name", None) == "arrow.uuid"

    storage = pa.array(
        [
            uuid.UUID("00000000-0000-0000-0000-000000000000").bytes,
            uuid.UUID("00000000-0000-0000-0000-000000000001").bytes,
        ],
        type=uuid_type.storage_type,
    )
    batch = pa.RecordBatch.from_arrays(
        [uuid_type.wrap_array(storage)],
        names=["uuid_col"],
    )

    df = ctx.create_dataframe([[batch]])
    result = df.select(second(first(column("uuid_col")))).collect()[0].column(0)

    expected = uuid_type.wrap_array(storage)

    if isinstance(result, pa.ChunkedArray):
        assert result.type.equals(uuid_type)
    else:
        assert isinstance(result, pa.ExtensionArray)
        assert result.type.equals(uuid_type)

    assert result.equals(expected)

    empty_storage = pa.array([], type=uuid_type.storage_type)
    empty_batch = pa.RecordBatch.from_arrays(
        [uuid_type.wrap_array(empty_storage)],
        names=["uuid_col"],
    )

    empty_first = udf(
        lambda values: pa.chunked_array([], type=uuid_type.storage_type),
        [uuid_field],
        uuid_field,
        volatility="immutable",
        name="uuid_empty_chunk",
    )

    empty_df = ctx.create_dataframe([[empty_batch]])
    empty_result = (
        empty_df.select(second(empty_first(column("uuid_col")))).collect()[0].column(0)
    )

    expected_empty = uuid_type.wrap_array(empty_storage)

    if isinstance(empty_result, pa.ChunkedArray):
        assert empty_result.type.equals(uuid_type)
        assert empty_result.combine_chunks().equals(expected_empty)
    else:
        assert isinstance(empty_result, pa.ExtensionArray)
        assert empty_result.type.equals(uuid_type)
        assert empty_result.equals(expected_empty)
