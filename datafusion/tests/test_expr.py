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
from datafusion.expr import Column, Literal, BinaryExpr, Projection
import pytest


@pytest.fixture
def test_ctx():
    ctx = SessionContext()
    ctx.register_csv("test", "testing/data/csv/aggregate_test_100.csv")
    return ctx


def test_logical_plan(test_ctx):
    df = test_ctx.sql("select c1, 123, c1 < 123 from test")
    plan = df.logical_plan()

    projection = plan.to_logical_node()
    assert isinstance(projection, Projection)

    expr = projection.projections()

    col1 = expr[0].to_logical_expr()
    assert isinstance(col1, Column)
    assert col1.name() == "c1"
    assert col1.qualified_name() == "test.c1"

    col2 = expr[1].to_logical_expr()
    assert isinstance(col2, Literal)
    assert col2.data_type() == "Int64"
    assert col2.value_i64() == 123

    col3 = expr[2].to_logical_expr()
    assert isinstance(col3, BinaryExpr)
    assert isinstance(col3.left().to_logical_expr(), Column)
    assert col3.op() == "<"
    assert isinstance(col3.right().to_logical_expr(), Literal)
