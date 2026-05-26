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


def test_array_transform_callable(df):
    expr = f.array_transform(col("a"), lambda v: v * 2)
    assert _column(df, expr, "d") == [[2, 4, 6], [8, 10]]


def test_array_transform_explicit_lambda(df):
    transform = f.lambda_(["v"], f.lambda_var("v") * lit(2))
    expr = f.array_transform(col("a"), transform)
    assert _column(df, expr, "d") == [[2, 4, 6], [8, 10]]


def test_array_transform_literal_body_is_coerced(df):
    expr = f.array_transform(col("a"), lambda v: 0)
    assert _column(df, expr, "z") == [[0, 0, 0], [0, 0]]


def test_list_transform_alias(df):
    expr = f.list_transform(col("a"), lambda v: v + 1)
    assert _column(df, expr, "d") == [[2, 3, 4], [5, 6]]


def test_array_any_match_callable(df):
    expr = f.array_any_match(col("a"), lambda v: v > 3)
    assert _column(df, expr, "m") == [False, True]


def test_array_any_match_explicit_lambda(df):
    predicate = f.lambda_(["v"], f.lambda_var("v") > lit(2))
    expr = f.array_any_match(col("a"), predicate)
    assert _column(df, expr, "m") == [True, True]


@pytest.mark.parametrize("alias", [f.any_match, f.list_any_match])
def test_any_match_aliases(df, alias):
    expr = alias(col("a"), lambda v: v > 4)
    assert _column(df, expr, "m") == [False, True]


def test_lambda_param_name_appears_in_plan(df):
    # The user-chosen parameter name should survive into the displayed plan
    # rather than a synthetic placeholder.
    expr = f.array_transform(col("a"), lambda value: value * 2)
    assert "value" in expr.canonical_name()


def test_to_lambda_rejects_non_callable():
    with pytest.raises(TypeError, match="expected an Expr or callable"):
        f.array_transform(col("a"), 42)


def test_to_lambda_rejects_zero_arg_callable():
    with pytest.raises(ValueError, match="at least one parameter"):
        f.array_transform(col("a"), lambda: lit(1))


def test_sql_lambda_requires_duckdb_dialect():
    # Lambda arrow syntax (``x -> ...``) is only parsed by dialects that
    # support lambda functions. The default Generic dialect treats ``->`` as
    # the JSON arrow operator, so ``x`` is read as a column reference.
    ctx = SessionContext()
    with pytest.raises(Exception, match="No field named x"):
        ctx.sql("select array_transform([1, 2, 3], x -> x * 2) as d").collect()

    duckdb_ctx = SessionContext(
        SessionConfig().set("datafusion.sql_parser.dialect", "DuckDB")
    )
    result = duckdb_ctx.sql(
        "select array_transform([1, 2, 3], x -> x * 2) as d"
    ).collect_column("d")
    assert result.to_pylist() == [[2, 4, 6]]


def test_pickle_lambda_expr_not_supported():
    # v1 limitation: upstream proto serialization rejects lambda expressions.
    expr = f.array_transform(col("a"), lambda v: v * 2)
    with pytest.raises(Exception, match="Lambda not implemented"):
        expr.to_bytes()
