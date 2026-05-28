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

import pyarrow as pa
import pyarrow.dataset as ds
from datafusion import Expr, SessionContext, Table, udtf
from datafusion.context import TableProviderExportable


def python_table_function_inner(
    num_cols: int, num_rows: int, num_batches: int
) -> Table:
    rows = list(range(num_rows))
    cols = [pa.array(rows) for _ in range(num_cols)]
    names = [f"row_{i}" for i in range(num_cols)]

    batch = pa.RecordBatch.from_arrays(cols, names=names)
    batches = [batch for _ in range(num_batches)]
    dataset = ds.dataset(batches)

    return Table(dataset)


def test_python_table_function_with_args_class() -> None:
    """Test Python TableFunction using a class with arguments."""
    ctx = SessionContext()

    class ParameterizedTableFunction:
        """Table function that takes parameters."""

        def __call__(self, num_cols: Expr, num_rows: Expr, num_batches: Expr) -> Table:
            return python_table_function_inner(
                num_cols.to_variant().value_i64(),
                num_rows.to_variant().value_i64(),
                num_batches.to_variant().value_i64(),
            )

    # Register the function
    table_func = ParameterizedTableFunction()
    table_udtf = udtf(table_func, "param_func")
    ctx.register_udtf(table_udtf)

    # Call with different parameters
    result = ctx.sql("SELECT * FROM param_func(5, 10, 2)").collect()
    assert len(result) == 2
    assert result[0].num_columns == 5
    assert result[0].num_rows == 10


def test_python_table_function_decorator() -> None:
    """Test Python TableFunction using decorator syntax."""
    ctx = SessionContext()

    @udtf("decorated_func")
    def my_decorated_func(num_cols: Expr, num_rows: Expr, num_batches: Expr) -> Table:
        return python_table_function_inner(
            num_cols.to_variant().value_i64(),
            num_rows.to_variant().value_i64(),
            num_batches.to_variant().value_i64(),
        )

    ctx.register_udtf(my_decorated_func)

    result = ctx.sql("SELECT * FROM decorated_func(3, 7, 2)").collect()
    assert len(result) == 2
    assert result[0].num_columns == 3
    assert result[0].num_rows == 7


def test_python_table_function_no_args() -> None:
    """Test Python TableFunction with no arguments."""
    ctx = SessionContext()

    @udtf("static_func")
    def static_table_func() -> Table:
        return python_table_function_inner(2, 3, 1)

    ctx.register_udtf(static_table_func)

    result = ctx.sql("SELECT * FROM static_func()").collect()
    assert len(result) == 1
    assert list(result[0].column(0).to_pylist()) == [0, 1, 2]
    assert list(result[0].column(1).to_pylist()) == [0, 1, 2]


def test_python_table_function_single_arg() -> None:
    """Test Python TableFunction with a single argument."""
    ctx = SessionContext()

    @udtf("single_arg_func")
    def single_arg_func(n: Expr) -> TableProviderExportable:
        return python_table_function_inner(2, n.to_variant().value_i64(), 1)

    ctx.register_udtf(single_arg_func)

    result = ctx.sql("SELECT * FROM single_arg_func(15)").collect()
    assert len(result) == 1
    assert result[0].num_columns == 2
    assert result[0].num_rows == 15


def test_python_table_function_with_string_args() -> None:
    """Test Python TableFunction with string arguments."""
    ctx = SessionContext()

    @udtf("string_arg_func")
    def string_arg_func(prefix: Expr) -> TableProviderExportable:
        prefix_str = prefix.to_variant().value_string()
        # Create a table with the prefix in column names

        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
            names=[f"{prefix_str}_a", f"{prefix_str}_b"],
        )

        return Table(ds.dataset([batch]))

    ctx.register_udtf(string_arg_func)

    result = ctx.sql("SELECT * FROM string_arg_func('test')").collect()
    assert len(result) == 1
    assert result[0].schema.names == ["test_a", "test_b"]


def test_python_table_function_receives_session() -> None:
    """A UDTF registered ``with_session=True`` gets the calling ctx."""
    ctx = SessionContext()
    captured: list[SessionContext] = []

    @udtf("session_aware_func", with_session=True)
    def session_aware_func(*, session: SessionContext) -> TableProviderExportable:
        captured.append(session)
        batch = pa.RecordBatch.from_pydict({"a": [1, 2, 3]})
        return Table(ds.dataset([batch]))

    ctx.register_udtf(session_aware_func)
    result = ctx.sql("SELECT * FROM session_aware_func()").collect()

    assert len(captured) == 1
    assert isinstance(captured[0], SessionContext)
    # Sharing the same catalog confirms the wrapper points at the caller's state.
    assert captured[0].catalog().schema().names() == ctx.catalog().schema().names()
    assert result[0].column(0).to_pylist() == [1, 2, 3]


def test_python_table_function_session_used_for_metadata() -> None:
    """The UDTF can inspect session state through the passed-in context."""
    ctx = SessionContext()
    base_batch = pa.RecordBatch.from_pydict({"x": [10, 20, 30]})
    ctx.register_batch("base_tbl", base_batch)

    seen_tables: list[set[str]] = []

    @udtf("table_inventory", with_session=True)
    def table_inventory(*, session: SessionContext) -> TableProviderExportable:
        # Stash the visible tables to verify the session wired through.
        seen_tables.append(session.catalog().schema().names())
        batch = pa.RecordBatch.from_pydict({"name": ["base_tbl"]})
        return Table(ds.dataset([batch]))

    ctx.register_udtf(table_inventory)
    result = ctx.sql("SELECT * FROM table_inventory()").collect()

    assert seen_tables == [{"base_tbl"}]
    assert result[0].column(0).to_pylist() == ["base_tbl"]


def test_python_table_function_class_callable_with_session() -> None:
    """Class-based UDTFs opt in via ``with_session=True``."""
    ctx = SessionContext()
    captured: list[SessionContext] = []

    class SessionAware:
        def __call__(
            self, n: Expr, *, session: SessionContext
        ) -> TableProviderExportable:
            captured.append(session)
            count = n.to_variant().value_i64()
            batch = pa.RecordBatch.from_pydict({"a": list(range(count))})
            return Table(ds.dataset([batch]))

    ctx.register_udtf(udtf(SessionAware(), "session_class_func", with_session=True))
    result = ctx.sql("SELECT * FROM session_class_func(3)").collect()

    assert len(captured) == 1
    assert isinstance(captured[0], SessionContext)
    assert result[0].column(0).to_pylist() == [0, 1, 2]


def test_python_table_function_without_session_flag_no_injection() -> None:
    """Default registration (no ``with_session``) calls func positionally."""
    ctx = SessionContext()

    @udtf("plain_func")
    def plain_func(n: Expr) -> TableProviderExportable:
        count = n.to_variant().value_i64()
        batch = pa.RecordBatch.from_pydict({"a": list(range(count))})
        return Table(ds.dataset([batch]))

    ctx.register_udtf(plain_func)
    result = ctx.sql("SELECT * FROM plain_func(4)").collect()

    assert result[0].column(0).to_pylist() == [0, 1, 2, 3]
