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

from datafusion import SessionContext, col
from datafusion.expr import Column, Literal, BinaryExpr, AggregateFunction
from datafusion.expr import (
    Projection,
    Filter,
    Aggregate,
    Limit,
    Sort,
    TableScan,
)
import pyarrow
import pytest


@pytest.fixture
def test_ctx():
    ctx = SessionContext()
    ctx.register_csv("test", "testing/data/csv/aggregate_test_100.csv")
    return ctx


def test_projection(test_ctx):
    df = test_ctx.sql("select c1, 123, c1 < 123 from test")
    plan = df.logical_plan()

    plan = plan.to_variant()
    assert isinstance(plan, Projection)

    expr = plan.projections()

    col1 = expr[0].to_variant()
    assert isinstance(col1, Column)
    assert col1.name() == "c1"
    assert col1.qualified_name() == "test.c1"

    col2 = expr[1].to_variant()
    assert isinstance(col2, Literal)
    assert col2.data_type() == "Int64"
    assert col2.value_i64() == 123

    col3 = expr[2].to_variant()
    assert isinstance(col3, BinaryExpr)
    assert isinstance(col3.left().to_variant(), Column)
    assert col3.op() == "<"
    assert isinstance(col3.right().to_variant(), Literal)

    plan = plan.input()[0].to_variant()
    assert isinstance(plan, TableScan)


def test_filter(test_ctx):
    df = test_ctx.sql("select c1 from test WHERE c1 > 5")
    plan = df.logical_plan()

    plan = plan.to_variant()
    assert isinstance(plan, Projection)

    plan = plan.input()[0].to_variant()
    assert isinstance(plan, Filter)


def test_limit(test_ctx):
    df = test_ctx.sql("select c1 from test LIMIT 10")
    plan = df.logical_plan()

    plan = plan.to_variant()
    assert isinstance(plan, Limit)
    assert plan.skip() == 0

    df = test_ctx.sql("select c1 from test LIMIT 10 OFFSET 5")
    plan = df.logical_plan()

    plan = plan.to_variant()
    assert isinstance(plan, Limit)
    assert plan.skip() == 5


def test_aggregate_query(test_ctx):
    df = test_ctx.sql("select c1, count(*) from test group by c1")
    plan = df.logical_plan()

    projection = plan.to_variant()
    assert isinstance(projection, Projection)

    aggregate = projection.input()[0].to_variant()
    assert isinstance(aggregate, Aggregate)

    col1 = aggregate.group_by_exprs()[0].to_variant()
    assert isinstance(col1, Column)
    assert col1.name() == "c1"
    assert col1.qualified_name() == "test.c1"

    col2 = aggregate.aggregate_exprs()[0].to_variant()
    assert isinstance(col2, AggregateFunction)


def test_sort(test_ctx):
    df = test_ctx.sql("select c1 from test order by c1")
    plan = df.logical_plan()

    plan = plan.to_variant()
    assert isinstance(plan, Sort)


def test_relational_expr(test_ctx):
    ctx = SessionContext()

    batch = pyarrow.RecordBatch.from_arrays(
        [pyarrow.array([1, 2, 3]), pyarrow.array(["alpha", "beta", "gamma"])],
        names=["a", "b"],
    )
    df = ctx.create_dataframe([[batch]], name="batch_array")

    assert df.filter(col("a") == 1).count() == 1
    assert df.filter(col("a") != 1).count() == 2
    assert df.filter(col("a") >= 1).count() == 3
    assert df.filter(col("a") > 1).count() == 2
    assert df.filter(col("a") <= 3).count() == 3
    assert df.filter(col("a") < 3).count() == 2

    assert df.filter(col("b") == "beta").count() == 1
    assert df.filter(col("b") != "beta").count() == 2

    assert df.filter(col("a") == "beta").count() == 0


def test_expr_to_variant():
    # Taken from https://github.com/apache/datafusion-python/issues/781
    from datafusion import SessionContext
    from datafusion.expr import Filter

    def traverse_logical_plan(plan):
        cur_node = plan.to_variant()
        if isinstance(cur_node, Filter):
            return cur_node.predicate().to_variant()
        if hasattr(plan, "inputs"):
            for input_plan in plan.inputs():
                res = traverse_logical_plan(input_plan)
                if res is not None:
                    return res

    ctx = SessionContext()
    data = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
    ctx.from_pydict(data, name="table1")
    query = "SELECT * FROM table1 t1 WHERE t1.name IN ('dfa', 'ad', 'dfre', 'vsa')"
    logical_plan = ctx.sql(query).optimized_logical_plan()
    variant = traverse_logical_plan(logical_plan)
    assert variant is not None
    assert variant.expr().to_variant().qualified_name() == "table1.name"
    assert (
        str(variant.list())
        == '[Expr(Utf8("dfa")), Expr(Utf8("ad")), Expr(Utf8("dfre")), Expr(Utf8("vsa"))]'
    )
    assert not variant.negated()


def test_expr_getitem() -> None:
    ctx = SessionContext()
    data = {
        "array_values": [[1, 2, 3], [4, 5], [6], []],
        "struct_values": [
            {"name": "Alice", "age": 15},
            {"name": "Bob", "age": 14},
            {"name": "Charlie", "age": 13},
            {"name": None, "age": 12},
        ],
    }
    df = ctx.from_pydict(data, name="table1")

    names = df.select(col("struct_values")["name"].alias("name")).collect()
    names = [r.as_py() for rs in names for r in rs["name"]]

    array_values = df.select(col("array_values")[1].alias("value")).collect()
    array_values = [r.as_py() for rs in array_values for r in rs["value"]]

    assert names == ["Alice", "Bob", "Charlie", None]
    assert array_values == [2, 5, None, None]


def test_display_name_deprecation():
    import warnings
    expr = col("foo")
    with warnings.catch_warnings(record=True) as w:
        # Cause all warnings to always be triggered
        warnings.simplefilter("always")

        # should trigger warning
        name = expr.display_name()

        # Verify some things
        assert len(w) == 1
        assert issubclass(w[-1].category, DeprecationWarning)
        assert "deprecated" in str(w[-1].message)

    # returns appropriate result
    assert name == expr.schema_name()
    assert name == "foo"
