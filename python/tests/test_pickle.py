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

"""Pickle / dill roundtrip tests for :py:class:`datafusion.Expr`.

The wire format is `datafusion-proto`'s ``LogicalExprNode``. Function
references are encoded by name, so unpickling resolves them against the
process-wide global :py:class:`SessionContext`. Tests that need a
non-built-in function temporarily install a custom global context and
restore the previous one.
"""

import pickle
from contextlib import contextmanager

import dill
import pyarrow as pa
import pytest
from datafusion import SessionContext, col, lit, udf
from datafusion import functions as f
from datafusion.expr import Expr


@pytest.fixture
def ctx():
    return SessionContext()


@pytest.fixture
def df(ctx):
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, None])],
        names=["a", "b"],
    )
    return ctx.create_dataframe([[batch]], name="t")


@contextmanager
def temporary_global_ctx(new_ctx):
    """Install ``new_ctx`` as the process-wide global and restore on exit."""
    previous = SessionContext.global_ctx()
    new_ctx.set_as_global()
    try:
        yield
    finally:
        previous.set_as_global()


@pytest.mark.parametrize("dumper", [pickle, dill], ids=["pickle", "dill"])
@pytest.mark.parametrize(
    "build_expr",
    [
        pytest.param(lambda: col("a"), id="column"),
        pytest.param(lambda: lit(42), id="literal_int"),
        pytest.param(lambda: lit("hello"), id="literal_str"),
        pytest.param(lambda: col("a") + lit(1), id="binary_add"),
        pytest.param(lambda: (col("a") * lit(2)) - col("b"), id="binary_nested"),
        pytest.param(lambda: col("a").alias("renamed"), id="alias"),
        pytest.param(lambda: col("a").cast(pa.float64()), id="cast"),
        pytest.param(lambda: col("a").is_null(), id="is_null"),
        pytest.param(lambda: col("a").between(lit(1), lit(10)), id="between"),
        pytest.param(lambda: ~(col("a") > lit(0)), id="not_gt"),
        pytest.param(lambda: f.sum(col("a")), id="agg_sum"),
        pytest.param(
            lambda: f.case(col("a")).when(lit(1), lit("one")).end(),
            id="case_when",
        ),
    ],
)
def test_builtin_roundtrip(build_expr, dumper):
    """Built-in expressions roundtrip via pickle and dill."""
    expr = build_expr()
    restored = dumper.loads(dumper.dumps(expr))
    assert isinstance(restored, Expr)
    # canonical_name() gives a full string form including function names,
    # so equal canonical names imply structural equivalence.
    assert restored.canonical_name() == expr.canonical_name()


@pytest.mark.parametrize("dumper", [pickle, dill], ids=["pickle", "dill"])
def test_pickled_expr_executes(df, dumper):
    """A roundtripped expression evaluates to the same result as the original."""
    expr = (col("a") + lit(10)).alias("a_plus_ten")
    restored = dumper.loads(dumper.dumps(expr))

    original = df.select(expr).collect()[0].column(0)
    after = df.select(restored).collect()[0].column(0)
    assert original == after
    assert original == pa.array([11, 12, 13], type=pa.int64())


def test_udf_roundtrip_via_global_ctx():
    """UDFs roundtrip when registered on the active global context.

    Mirrors the documented usage of ``SessionContext.set_as_global``.
    """
    is_null = udf(
        lambda x: x.is_null(),
        [pa.int64()],
        pa.bool_(),
        volatility="immutable",
        name="pickle_test_is_null",
    )

    custom_ctx = SessionContext()
    custom_ctx.register_udf(is_null)

    expr = is_null(col("b"))

    with temporary_global_ctx(custom_ctx):
        data = pickle.dumps(expr)
        restored = pickle.loads(data)  # noqa: S301
        assert restored.canonical_name() == expr.canonical_name()

        # Also evaluate to confirm the UDF body is wired up post-roundtrip.
        batch = pa.RecordBatch.from_arrays([pa.array([1, None, 3])], names=["b"])
        df = custom_ctx.create_dataframe([[batch]], name="t_udf")
        result = df.select(restored.alias("nul")).collect()[0].column(0)
        assert result == pa.array([False, True, False])


def test_udf_roundtrip_fails_without_registration():
    """Without the UDF registered on the global context, unpickle errors out
    rather than silently substituting a different implementation."""
    is_null = udf(
        lambda x: x.is_null(),
        [pa.int64()],
        pa.bool_(),
        volatility="immutable",
        name="pickle_test_unknown_udf",
    )
    expr = is_null(col("b"))

    data = pickle.dumps(expr)
    # The default global ctx does not have this UDF registered. Reconstruction
    # must raise rather than silently substitute a placeholder. DataFusion
    # surfaces this as a generic Python ``Exception`` whose message names the
    # missing function, so match on the function name.
    with pytest.raises(Exception, match="pickle_test_unknown_udf"):
        pickle.loads(data)  # noqa: S301


def test_getstate_returns_bytes():
    """``__getstate__`` is exposed directly and returns raw bytes — useful for
    callers that want to persist or transmit expressions without pickle."""
    expr = col("a") + lit(1)
    state = expr.__getstate__()
    assert isinstance(state, bytes)
    assert len(state) > 0

    rebuilt = Expr.__new__(Expr)
    rebuilt.__setstate__(state)
    assert rebuilt.canonical_name() == expr.canonical_name()
