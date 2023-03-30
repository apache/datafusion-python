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
import pytest

from datafusion import functions as f
from datafusion import DataFrame, SessionContext, column, literal, udf


@pytest.fixture
def ctx():
    return SessionContext()


@pytest.fixture
def df():
    ctx = SessionContext()

    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6]), pa.array([8, 5, 8])],
        names=["a", "b", "c"],
    )

    return ctx.create_dataframe([[batch]])


@pytest.fixture
def struct_df():
    ctx = SessionContext()

    # create a RecordBatch and a new DataFrame from it
    batch = pa.RecordBatch.from_arrays(
        [pa.array([{"c": 1}, {"c": 2}, {"c": 3}]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )

    return ctx.create_dataframe([[batch]])


@pytest.fixture
def aggregate_df():
    ctx = SessionContext()
    ctx.register_csv("test", "testing/data/csv/aggregate_test_100.csv")
    return ctx.sql("select c1, sum(c2) from test group by c1")


def test_select(df):
    df = df.select(
        column("a") + column("b"),
        column("a") - column("b"),
    )

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert result.column(0) == pa.array([5, 7, 9])
    assert result.column(1) == pa.array([-3, -3, -3])


def test_select_columns(df):
    df = df.select_columns("b", "a")

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert result.column(0) == pa.array([4, 5, 6])
    assert result.column(1) == pa.array([1, 2, 3])


def test_filter(df):
    df = df.select(
        column("a") + column("b"),
        column("a") - column("b"),
    ).filter(column("a") > literal(2))

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert result.column(0) == pa.array([9])
    assert result.column(1) == pa.array([-3])


def test_sort(df):
    df = df.sort(column("b").sort(ascending=False))

    table = pa.Table.from_batches(df.collect())
    expected = {"a": [3, 2, 1], "b": [6, 5, 4], "c": [8, 5, 8]}

    assert table.to_pydict() == expected


def test_limit(df):
    df = df.limit(1)

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert len(result.column(0)) == 1
    assert len(result.column(1)) == 1


def test_with_column(df):
    df = df.with_column("c", column("a") + column("b"))

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert result.schema.field(0).name == "a"
    assert result.schema.field(1).name == "b"
    assert result.schema.field(2).name == "c"

    assert result.column(0) == pa.array([1, 2, 3])
    assert result.column(1) == pa.array([4, 5, 6])
    assert result.column(2) == pa.array([5, 7, 9])


def test_with_column_renamed(df):
    df = df.with_column("c", column("a") + column("b")).with_column_renamed(
        "c", "sum"
    )

    result = df.collect()[0]

    assert result.schema.field(0).name == "a"
    assert result.schema.field(1).name == "b"
    assert result.schema.field(2).name == "sum"


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


def test_join():
    ctx = SessionContext()

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    df = ctx.create_dataframe([[batch]], "l")

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2]), pa.array([8, 10])],
        names=["a", "c"],
    )
    df1 = ctx.create_dataframe([[batch]], "r")

    df = df.join(df1, join_keys=(["a"], ["a"]), how="inner")
    df.show()
    df = df.sort(column("l.a").sort(ascending=True))
    table = pa.Table.from_batches(df.collect())

    expected = {"a": [1, 2], "c": [8, 10], "b": [4, 5]}
    assert table.to_pydict() == expected


def test_distinct():
    ctx = SessionContext()

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3, 1, 2, 3]), pa.array([4, 5, 6, 4, 5, 6])],
        names=["a", "b"],
    )
    df_a = (
        ctx.create_dataframe([[batch]])
        .distinct()
        .sort(column("a").sort(ascending=True))
    )

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    df_b = ctx.create_dataframe([[batch]]).sort(
        column("a").sort(ascending=True)
    )

    assert df_a.collect() == df_b.collect()


def test_window_functions(df):
    df = df.select(
        column("a"),
        column("b"),
        column("c"),
        f.alias(
            f.window("row_number", [], order_by=[f.order_by(column("c"))]),
            "row",
        ),
        f.alias(
            f.window("rank", [], order_by=[f.order_by(column("c"))]),
            "rank",
        ),
        f.alias(
            f.window("dense_rank", [], order_by=[f.order_by(column("c"))]),
            "dense_rank",
        ),
        f.alias(
            f.window("percent_rank", [], order_by=[f.order_by(column("c"))]),
            "percent_rank",
        ),
        f.alias(
            f.window("cume_dist", [], order_by=[f.order_by(column("b"))]),
            "cume_dist",
        ),
        f.alias(
            f.window(
                "ntile", [literal(2)], order_by=[f.order_by(column("c"))]
            ),
            "ntile",
        ),
        f.alias(
            f.window("lag", [column("b")], order_by=[f.order_by(column("b"))]),
            "previous",
        ),
        f.alias(
            f.window(
                "lead", [column("b")], order_by=[f.order_by(column("b"))]
            ),
            "next",
        ),
        f.alias(
            f.window(
                "first_value",
                [column("a")],
                order_by=[f.order_by(column("b"))],
            ),
            "first_value",
        ),
        f.alias(
            f.window(
                "last_value", [column("b")], order_by=[f.order_by(column("b"))]
            ),
            "last_value",
        ),
        f.alias(
            f.window(
                "nth_value",
                [column("b"), literal(2)],
                order_by=[f.order_by(column("b"))],
            ),
            "2nd_value",
        ),
    )

    table = pa.Table.from_batches(df.collect())

    expected = {
        "a": [1, 2, 3],
        "b": [4, 5, 6],
        "c": [8, 5, 8],
        "row": [2, 1, 3],
        "rank": [2, 1, 2],
        "dense_rank": [2, 1, 2],
        "percent_rank": [0.5, 0, 0.5],
        "cume_dist": [0.3333333333333333, 0.6666666666666666, 1.0],
        "ntile": [1, 1, 2],
        "next": [5, 6, None],
        "previous": [None, 4, 5],
        "first_value": [1, 1, 1],
        "last_value": [4, 5, 6],
        "2nd_value": [None, 5, 5],
    }
    assert table.sort_by("a").to_pydict() == expected


def test_get_dataframe(tmp_path):
    ctx = SessionContext()

    path = tmp_path / "test.csv"
    table = pa.Table.from_arrays(
        [
            [1, 2, 3, 4],
            ["a", "b", "c", "d"],
            [1.1, 2.2, 3.3, 4.4],
        ],
        names=["int", "str", "float"],
    )
    pa.csv.write_csv(table, path)

    ctx.register_csv("csv", path)

    df = ctx.table("csv")
    assert isinstance(df, DataFrame)


def test_struct_select(struct_df):
    df = struct_df.select(
        column("a")["c"] + column("b"),
        column("a")["c"] - column("b"),
    )

    # execute and collect the first (and only) batch
    result = df.collect()[0]

    assert result.column(0) == pa.array([5, 7, 9])
    assert result.column(1) == pa.array([-3, -3, -3])


def test_explain(df):
    df = df.select(
        column("a") + column("b"),
        column("a") - column("b"),
    )
    df.explain()


def test_logical_plan(aggregate_df):
    plan = aggregate_df.logical_plan()

    expected = "Projection: test.c1, SUM(test.c2)"

    assert expected == plan.display()

    expected = (
        "Projection: test.c1, SUM(test.c2)\n"
        "  Aggregate: groupBy=[[test.c1]], aggr=[[SUM(test.c2)]]\n"
        "    TableScan: test"
    )

    assert expected == plan.display_indent()


def test_optimized_logical_plan(aggregate_df):
    plan = aggregate_df.optimized_logical_plan()

    expected = "Aggregate: groupBy=[[test.c1]], aggr=[[SUM(test.c2)]]"

    assert expected == plan.display()

    expected = (
        "Aggregate: groupBy=[[test.c1]], aggr=[[SUM(test.c2)]]\n"
        "  TableScan: test projection=[c1, c2]"
    )

    assert expected == plan.display_indent()


def test_execution_plan(aggregate_df):
    plan = aggregate_df.execution_plan()

    expected = "AggregateExec: mode=FinalPartitioned, gby=[c1@0 as c1], aggr=[SUM(test.c2)]\n"  # noqa: E501

    assert expected == plan.display()

    expected = (
        "ProjectionExec: expr=[c1@0 as c1, SUM(test.c2)@1 as SUM(test.c2)]\n"
        "  Aggregate: groupBy=[[test.c1]], aggr=[[SUM(test.c2)]]\n"
        "    TableScan: test projection=[c1, c2]"
    )

    indent = plan.display_indent()

    # indent plan will be different for everyone due to absolute path
    # to filename, so we just check for some expected content
    assert "AggregateExec:" in indent
    assert "CoalesceBatchesExec:" in indent
    assert "RepartitionExec:" in indent
    assert "CsvExec:" in indent

    ctx = SessionContext()
    stream = ctx.execute(plan, 0)
    # get the one and only batch
    batch = stream.next()
    assert batch is not None
    # there should be no more batches
    batch = stream.next()
    assert batch is None


def test_repartition(df):
    df.repartition(2)


def test_repartition_by_hash(df):
    df.repartition_by_hash(column("a"), num=2)


def test_intersect():
    ctx = SessionContext()

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    df_a = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([3, 4, 5]), pa.array([6, 7, 8])],
        names=["a", "b"],
    )
    df_b = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([3]), pa.array([6])],
        names=["a", "b"],
    )
    df_c = ctx.create_dataframe([[batch]]).sort(
        column("a").sort(ascending=True)
    )

    df_a_i_b = df_a.intersect(df_b).sort(column("a").sort(ascending=True))

    assert df_c.collect() == df_a_i_b.collect()


def test_except_all():
    ctx = SessionContext()

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    df_a = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([3, 4, 5]), pa.array([6, 7, 8])],
        names=["a", "b"],
    )
    df_b = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2]), pa.array([4, 5])],
        names=["a", "b"],
    )
    df_c = ctx.create_dataframe([[batch]]).sort(
        column("a").sort(ascending=True)
    )

    df_a_e_b = df_a.except_all(df_b).sort(column("a").sort(ascending=True))

    assert df_c.collect() == df_a_e_b.collect()


def test_collect_partitioned():
    ctx = SessionContext()

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )

    assert [[batch]] == ctx.create_dataframe([[batch]]).collect_partitioned()


def test_union(ctx):
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    df_a = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([3, 4, 5]), pa.array([6, 7, 8])],
        names=["a", "b"],
    )
    df_b = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3, 3, 4, 5]), pa.array([4, 5, 6, 6, 7, 8])],
        names=["a", "b"],
    )
    df_c = ctx.create_dataframe([[batch]]).sort(
        column("a").sort(ascending=True)
    )

    df_a_u_b = df_a.union(df_b).sort(column("a").sort(ascending=True))

    assert df_c.collect() == df_a_u_b.collect()


def test_union_distinct(ctx):
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    df_a = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([3, 4, 5]), pa.array([6, 7, 8])],
        names=["a", "b"],
    )
    df_b = ctx.create_dataframe([[batch]])

    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3, 4, 5]), pa.array([4, 5, 6, 7, 8])],
        names=["a", "b"],
    )
    df_c = ctx.create_dataframe([[batch]]).sort(
        column("a").sort(ascending=True)
    )

    df_a_u_b = df_a.union(df_b, True).sort(column("a").sort(ascending=True))

    assert df_c.collect() == df_a_u_b.collect()
    assert df_c.collect() == df_a_u_b.collect()


def test_cache(df):
    assert df.cache().collect() == df.collect()


def test_count(df):
    # Get number of rows
    assert df.count() == 3


def test_to_pandas(df):
    # Skip test if pandas is not installed
    pd = pytest.importorskip("pandas")

    # Convert datafusion dataframe to pandas dataframe
    pandas_df = df.to_pandas()
    assert type(pandas_df) == pd.DataFrame
    assert pandas_df.shape == (3, 3)
    assert set(pandas_df.columns) == {"a", "b", "c"}


def test_empty_to_pandas(df):
    # Skip test if pandas is not installed
    pd = pytest.importorskip("pandas")

    # Convert empty datafusion dataframe to pandas dataframe
    pandas_df = df.limit(0).to_pandas()
    assert type(pandas_df) == pd.DataFrame
    assert pandas_df.shape == (0, 3)
    assert set(pandas_df.columns) == {"a", "b", "c"}


def test_to_polars(df):
    # Skip test if polars is not installed
    pl = pytest.importorskip("polars")

    # Convert datafusion dataframe to polars dataframe
    polars_df = df.to_polars()
    assert type(polars_df) == pl.DataFrame
    assert polars_df.shape == (3, 3)
    assert set(polars_df.columns) == {"a", "b", "c"}


def test_empty_to_polars(df):
    # Skip test if polars is not installed
    pl = pytest.importorskip("polars")

    # Convert empty datafusion dataframe to polars dataframe
    polars_df = df.limit(0).to_polars()
    assert type(polars_df) == pl.DataFrame
    assert polars_df.shape == (0, 3)
    assert set(polars_df.columns) == {"a", "b", "c"}


def test_to_arrow_table(df):
    # Convert datafusion dataframe to pyarrow Table
    pyarrow_table = df.to_arrow_table()
    assert type(pyarrow_table) == pa.Table
    assert pyarrow_table.shape == (3, 3)
    assert set(pyarrow_table.column_names) == {"a", "b", "c"}


def test_empty_to_arrow_table(df):
    # Convert empty datafusion dataframe to pyarrow Table
    pyarrow_table = df.limit(0).to_arrow_table()
    assert type(pyarrow_table) == pa.Table
    assert pyarrow_table.shape == (0, 3)
    assert set(pyarrow_table.column_names) == {"a", "b", "c"}


def test_to_pylist(df):
    # Convert datafusion dataframe to Python list
    pylist = df.to_pylist()
    assert type(pylist) == list
    assert pylist == [
        {"a": 1, "b": 4, "c": 8},
        {"a": 2, "b": 5, "c": 5},
        {"a": 3, "b": 6, "c": 8},
    ]


def test_to_pydict(df):
    # Convert datafusion dataframe to Python dictionary
    pydict = df.to_pydict()
    assert type(pydict) == dict
    assert pydict == {"a": [1, 2, 3], "b": [4, 5, 6], "c": [8, 5, 8]}


def test_describe(df):

    # Calculate statistics
    df = df.describe()

    # Collect the result
    result = df.to_pydict()

    assert result == {
        "describe": [
            "count",
            "null_count",
            "mean",
            "std",
            "min",
            "max",
            "median",
        ],
        "a": [3.0, 3.0, 2.0, 1.0, 1.0, 3.0, 2.0],
        "b": [3.0, 3.0, 5.0, 1.0, 4.0, 6.0, 5.0],
        "c": [3.0, 3.0, 7.0, 1.7320508075688772, 5.0, 8.0, 8.0],
    }
