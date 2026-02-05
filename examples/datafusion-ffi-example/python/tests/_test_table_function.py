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

from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import Expr, SessionContext, udtf
from datafusion_ffi_example import MyTableFunction, MyTableProvider

if TYPE_CHECKING:
    from datafusion.context import TableProviderExportable


def test_ffi_table_function_register() -> None:
    ctx = SessionContext()
    table_func = MyTableFunction()

    table_udtf = udtf(table_func, "my_table_func")
    ctx.register_udtf(table_udtf)
    result = ctx.sql("select * from my_table_func()").collect()

    assert len(result) == 2
    assert result[0].num_columns == 4
    print(result)

    result = [r.column(0) for r in result]
    expected = [
        pa.array([0, 1, 2], type=pa.int32()),
        pa.array([3, 4, 5, 6], type=pa.int32()),
    ]

    assert result == expected


def test_ffi_table_function_call_directly():
    ctx = SessionContext()
    table_func = MyTableFunction()
    table_udtf = udtf(table_func, "my_table_func")

    my_table = table_udtf()
    ctx.register_table("t", my_table)
    result = ctx.table("t").collect()

    assert len(result) == 2
    assert result[0].num_columns == 4
    print(result)

    result = [r.column(0) for r in result]
    expected = [
        pa.array([0, 1, 2], type=pa.int32()),
        pa.array([3, 4, 5, 6], type=pa.int32()),
    ]

    assert result == expected


class PythonTableFunction:
    """Python based table function.

    This class is used as a Python implementation of a table function.
    We use the existing TableProvider to create the underlying
    provider, and this function takes no arguments
    """

    def __call__(
        self, num_cols: Expr, num_rows: Expr, num_batches: Expr
    ) -> TableProviderExportable:
        args = [
            num_cols.to_variant().value_i64(),
            num_rows.to_variant().value_i64(),
            num_batches.to_variant().value_i64(),
        ]
        return MyTableProvider(*args)


def common_table_function_test(test_ctx: SessionContext) -> None:
    result = test_ctx.sql("select * from my_table_func(3,2,4)").collect()

    assert len(result) == 4
    assert result[0].num_columns == 3
    print(result)

    result = [r.column(0) for r in result]
    expected = [
        pa.array([0, 1], type=pa.int32()),
        pa.array([2, 3, 4], type=pa.int32()),
        pa.array([4, 5, 6, 7], type=pa.int32()),
        pa.array([6, 7, 8, 9, 10], type=pa.int32()),
    ]

    assert result == expected


def test_python_table_function():
    ctx = SessionContext()
    table_func = PythonTableFunction()
    table_udtf = udtf(table_func, "my_table_func")
    ctx.register_udtf(table_udtf)

    common_table_function_test(ctx)


def test_python_table_function_decorator():
    ctx = SessionContext()

    @udtf("my_table_func")
    def my_udtf(
        num_cols: Expr, num_rows: Expr, num_batches: Expr
    ) -> TableProviderExportable:
        args = [
            num_cols.to_variant().value_i64(),
            num_rows.to_variant().value_i64(),
            num_batches.to_variant().value_i64(),
        ]
        return MyTableProvider(*args)

    ctx.register_udtf(my_udtf)

    common_table_function_test(ctx)
