from datafusion import SessionContext, col, literal
import pyarrow as pa
import pytest


def test_register_filtered_dataframe():
    ctx = SessionContext()

    data = {"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]}

    df = ctx.from_pydict(data, "my_table")

    df_filtered = df.filter(col("a") > literal(2))
    view = df_filtered.into_view()

    assert view.kind == "view"

    ctx.register_table("view1", view)

    df_view = ctx.sql("SELECT * FROM view1")

    filtered_results = df_view.collect()

    result_dicts = [batch.to_pydict() for batch in filtered_results]

    expected_results = [{"a": [3, 4, 5], "b": [30, 40, 50]}]

    assert result_dicts == expected_results

    df_results = df.collect()

    df_result_dicts = [batch.to_pydict() for batch in df_results]

    expected_df_results = [{"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]}]

    assert df_result_dicts == expected_df_results
