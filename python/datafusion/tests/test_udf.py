from datafusion import udf, column
import pyarrow as pa
import pytest


@pytest.fixture
def df(ctx):
    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 4, 6])],
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

    df = df.select(is_null(column("a")))
    result = df.collect()[0].column(0)

    assert result == pa.array([False, False, False])


def test_register_udf(ctx, df) -> None:
    is_null = udf(
        lambda x: x.is_null(),
        [pa.float64()],
        pa.bool_(),
        volatility="immutable",
        name="is_null",
    )

    ctx.register_udf(is_null)

    df_result = ctx.sql("select is_null(a) from test_table")
    result = df_result.collect()[0].column(0)

    assert result == pa.array([False, False, False])


class OverThresholdUDF:
    def __init__(self, threshold: int = 0) -> None:
        self.threshold = threshold

    def __call__(self, values: pa.Array) -> pa.Array:
        return pa.array(v.as_py() >= self.threshold for v in values)


def test_udf_with_parameters(df) -> None:
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
