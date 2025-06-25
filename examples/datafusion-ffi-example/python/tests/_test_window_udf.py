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
from datafusion import SessionContext, col, udwf
from datafusion_ffi_example import MyRankUDF


def setup_context_with_table():
    ctx = SessionContext()

    # Pick numbers here so we get the same value in both groups
    # since we cannot be certain of the output order of batches
    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([40, 10, 30, 20], type=pa.int64()),
        ],
        names=["a"],
    )
    ctx.register_record_batches("test_table", [[batch]])
    return ctx


def test_ffi_window_register():
    ctx = setup_context_with_table()
    my_udwf = udwf(MyRankUDF())
    ctx.register_udwf(my_udwf)

    result = ctx.sql(
        "select a, my_custom_rank() over (order by a) from test_table"
    ).collect()
    assert len(result) == 1
    assert result[0].num_columns == 2

    results = [
        (result[0][0][idx].as_py(), result[0][1][idx].as_py()) for idx in range(4)
    ]
    results.sort()

    expected = [
        (10, 1),
        (20, 2),
        (30, 3),
        (40, 4),
    ]
    assert results == expected


def test_ffi_window_call_directly():
    ctx = setup_context_with_table()
    my_udwf = udwf(MyRankUDF())

    result = (
        ctx.table("test_table")
        .select(col("a"), my_udwf().order_by(col("a")).build())
        .collect()
    )

    assert len(result) == 1
    assert result[0].num_columns == 2

    results = [
        (result[0][0][idx].as_py(), result[0][1][idx].as_py()) for idx in range(4)
    ]
    results.sort()

    expected = [
        (10, 1),
        (20, 2),
        (30, 3),
        (40, 4),
    ]
    assert results == expected
