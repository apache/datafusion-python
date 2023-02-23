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

from datafusion import SessionContext
from datafusion.expr import Column, Literal, BinaryExpr, AggregateFunction
from datafusion.expr import (
    Projection,
    Filter,
    Aggregate,
    Limit,
    Sort,
    TableScan,
)
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
