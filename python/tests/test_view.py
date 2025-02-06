"""
This script demonstrates how to register a filtered DataFrame as a table
using DataFusion's `ctx.register_table` method and then query it.
"""

from datafusion import SessionContext, col, literal
import pyarrow as pa
import pytest

def test_register_filtered_dataframe():
    # Create a new session context
    ctx = SessionContext()

    # Create sample data as a dictionary
    data = {
        "a": [1, 2, 3, 4, 5],
        "b": [10, 20, 30, 40, 50]
    }

    # Create a DataFrame from the dictionary
    df = ctx.from_pydict(data, "my_table")

    # Filter the DataFrame (for example, keep rows where a > 2)
    df_filtered = df.filter(col("a") > literal(2))

    # Register the filtered DataFrame as a table called "view1"
    ctx.register_table("view1", df_filtered)

    # Now run a SQL query against the registered table "view1"
    df_view = ctx.sql("SELECT * FROM view1")

    # Collect the results (as a list of Arrow RecordBatches)
    results = df_view.collect()

    # Convert results to a list of dictionaries for easier assertion
    result_dicts = [batch.to_pydict() for batch in results]

    # Expected results
    expected_results = [
        {"a": [3, 4, 5], "b": [30, 40, 50]}
    ]

    # Assert the results match the expected results
    assert result_dicts == expected_results
