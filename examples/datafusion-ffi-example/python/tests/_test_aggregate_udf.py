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

import pyarrow as pa
from datafusion import SessionContext, col, udaf
from datafusion_ffi_example import MySumUDF


def setup_context_with_table():
    ctx = SessionContext()

    # Pick numbers here so we get the same value in both groups
    # since we cannot be certain of the output order of batches
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1, 2, 3, None], type=pa.int64()),
            pa.array([1, 1, 2, 2], type=pa.int64()),
        ],
        names=["a", "b"],
    )
    ctx.register_record_batches("test_table", [[batch]])
    return ctx


def test_ffi_aggregate_register():
    ctx = setup_context_with_table()
    my_udaf = udaf(MySumUDF())
    ctx.register_udaf(my_udaf)

    result = ctx.sql("select my_custom_sum(a) from test_table group by b").collect()

    assert len(result) == 2
    assert result[0].num_columns == 1

    result = [r.column(0) for r in result]
    expected = [
        pa.array([3], type=pa.int64()),
        pa.array([3], type=pa.int64()),
    ]

    assert result == expected


def test_ffi_aggregate_call_directly():
    ctx = setup_context_with_table()
    my_udaf = udaf(MySumUDF())

    result = (
        ctx.table("test_table").aggregate([col("b")], [my_udaf(col("a"))]).collect()
    )

    assert len(result) == 2
    assert result[0].num_columns == 2

    result = [r.column(1) for r in result]
    expected = [
        pa.array([3], type=pa.int64()),
        pa.array([3], type=pa.int64()),
    ]

    assert result == expected
