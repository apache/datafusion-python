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
"""Tests for lambda expressions and higher-order array functions."""

import pytest
from datafusion import SessionConfig, SessionContext, col, lit
from datafusion import functions as f


@pytest.fixture
def df():
    ctx = SessionContext()
    return ctx.from_pydict({"a": [[1, 2, 3], [4, 5]]})


def _column(df, expr, name):
    return df.select(expr.alias(name)).collect_column(name).to_pylist()


@pytest.mark.parametrize(
    ("build_expr", "expected"),
    [
        pytest.param(
            lambda: f.array_transform(col("a"), lambda v: v * 2),
            [[2, 4, 6], [8, 10]],
            id="array_transform_callable",
        ),
        pytest.param(
            lambda: f.array_transform(
                col("a"), f.lambda_(["v"], f.lambda_var("v") * lit(2))
            ),
            [[2, 4, 6], [8, 10]],
            id="array_transform_explicit_lambda",
        ),
        pytest.param(
            lambda: f.array_transform(col("a"), lambda v: 0),
            [[0, 0, 0], [0, 0]],
            id="array_transform_literal_body_is_coerced",
        ),
        pytest.param(
            lambda: f.list_transform(col("a"), lambda v: v + 1),
            [[2, 3, 4], [5, 6]],
            id="list_transform_alias",
        ),
        pytest.param(
            lambda: f.array_any_match(col("a"), lambda v: v > 3),
            [False, True],
            id="array_any_match_callable",
        ),
        pytest.param(
            lambda: f.array_any_match(
                col("a"), f.lambda_(["v"], f.lambda_var("v") > lit(2))
            ),
            [True, True],
            id="array_any_match_explicit_lambda",
        ),
        pytest.param(
            lambda: f.any_match(col("a"), lambda v: v > 4),
            [False, True],
            id="any_match_alias",
        ),
        pytest.param(
            lambda: f.list_any_match(col("a"), lambda v: v > 4),
            [False, True],
            id="list_any_match_alias",
        ),
        pytest.param(
            lambda: f.array_filter(col("a"), lambda v: v > 2),
            [[3], [4, 5]],
            id="array_filter_callable",
        ),
        pytest.param(
            lambda: f.array_filter(
                col("a"), f.lambda_(["v"], f.lambda_var("v") > lit(2))
            ),
            [[3], [4, 5]],
            id="array_filter_explicit_lambda",
        ),
        pytest.param(
            lambda: f.list_filter(col("a"), lambda v: v > 2),
            [[3], [4, 5]],
            id="list_filter_alias",
        ),
    ],
)
def test_higher_order_function_results(df, build_expr, expected):
    assert _column(df, build_expr(), "r") == expected


def test_lambda_param_name_appears_in_plan(df):
    # The user-chosen parameter name should survive into the displayed plan
    # rather than a synthetic placeholder.
    expr = f.array_transform(col("a"), lambda value: value * 2)
    assert "value" in expr.canonical_name()


@pytest.mark.parametrize(
    ("arg", "exc_type", "match"),
    [
        pytest.param(42, TypeError, "expected an Expr or callable", id="non_callable"),
        pytest.param(
            lambda: lit(1),
            ValueError,
            "at least one parameter",
            id="zero_arg_callable",
        ),
    ],
)
def test_to_lambda_rejects_invalid_arg(arg, exc_type, match):
    with pytest.raises(exc_type, match=match):
        f.array_transform(col("a"), arg)


@pytest.mark.parametrize("dialect", ["DuckDB", "ClickHouse", "Snowflake", "Databricks"])
def test_sql_lambda_keyword_syntax(dialect):
    # ``lambda x: x * 2`` is the forward-compatible syntax. DuckDB will drop
    # the arrow form (``x -> ...``) in v2.1; the keyword form is supported by
    # every dialect in sqlparser-rs that enables lambda functions.
    ctx = SessionContext(SessionConfig().set("datafusion.sql_parser.dialect", dialect))
    result = ctx.sql(
        "select array_transform([1, 2, 3], lambda x: x * 2) as d"
    ).collect_column("d")
    assert result.to_pylist() == [[2, 4, 6]]


def test_pickle_lambda_expr_not_supported():
    # v1 limitation: upstream proto serialization rejects lambda expressions.
    expr = f.array_transform(col("a"), lambda v: v * 2)
    with pytest.raises(Exception, match="Lambda not implemented"):
        expr.to_bytes()
