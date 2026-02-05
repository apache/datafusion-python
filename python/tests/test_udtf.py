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
