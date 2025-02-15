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
from datafusion import SessionContext, col, udf
from datafusion_ffi_example import IsNullUDF


def setup_context_with_table():
    ctx = SessionContext()

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3, None])],
        names=["a"],
    )
    ctx.register_record_batches("test_table", [[batch]])
    return ctx


def test_ffi_scalar_register():
    ctx = setup_context_with_table()
    my_udf = udf(IsNullUDF())
    ctx.register_udf(my_udf)

    result = ctx.sql("select my_custom_is_null(a) from test_table").collect()

    assert len(result) == 1
    assert result[0].num_columns == 1
    print(result)

    result = [r.column(0) for r in result]
    expected = [
        pa.array([False, False, False, True], type=pa.bool_()),
    ]

    assert result == expected


def test_ffi_scalar_call_directly():
    ctx = setup_context_with_table()
    my_udf = udf(IsNullUDF())

    result = ctx.table("test_table").select(my_udf(col("a"))).collect()

    assert len(result) == 1
    assert result[0].num_columns == 1
    print(result)

    result = [r.column(0) for r in result]
    expected = [
        pa.array([False, False, False, True], type=pa.bool_()),
    ]

    assert result == expected
