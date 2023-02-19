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
from datafusion.expr import Projection, Filter, Aggregate, Limit, Sort, TableScan
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

    plan = plan.input().to_variant()
    assert isinstance(plan, TableScan)


def test_filter(test_ctx):
    df = test_ctx.sql("select c1 from test WHERE c1 > 5")
    plan = df.logical_plan()

    plan = plan.to_variant()
    assert isinstance(plan, Projection)

    plan = plan.input().to_variant()
    assert isinstance(plan, Filter)


def test_limit(test_ctx):
    df = test_ctx.sql("select c1 from test LIMIT 10")
    plan = df.logical_plan()

    plan = plan.to_variant()
    assert isinstance(plan, Limit)


def test_aggregate(test_ctx):
    df = test_ctx.sql("select c1, COUNT(*) from test GROUP BY c1")
    plan = df.logical_plan()

    plan = plan.to_variant()
    assert isinstance(plan, Projection)

    plan = plan.input().to_variant()
    assert isinstance(plan, Aggregate)


