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

"""In-process pickle round-trip tests for :class:`Expr`.

Built-in functions and Python UDFs (scalar, aggregate, window) travel
with the pickled expression and do not need worker-side pre-registration.
The worker context (:mod:`datafusion.ipc`) is only consulted for UDFs
imported via the FFI capsule protocol.
"""

from __future__ import annotations

import pickle

import pyarrow as pa
import pytest
from datafusion import Expr, SessionContext, col, lit, udf
from datafusion.ipc import (
    clear_worker_ctx,
    set_worker_ctx,
)


@pytest.fixture(autouse=True)
def _reset_worker_ctx():
    """Ensure every test starts with no worker context installed."""
    clear_worker_ctx()
    yield
    clear_worker_ctx()


def _double_udf():
    return udf(
        lambda arr: pa.array([(v.as_py() or 0) * 2 for v in arr]),
        [pa.int64()],
        pa.int64(),
        volatility="immutable",
        name="double",
    )


class TestProtoRoundTrip:
    def test_builtin_round_trip(self):
        e = col("a") + lit(1)
        blob = pickle.dumps(e)
        decoded = pickle.loads(blob)  # noqa: S301
        assert decoded.canonical_name() == e.canonical_name()

    def test_to_bytes_from_bytes(self):
        e = col("x") * lit(7)
        blob = e.to_bytes()
        assert isinstance(blob, bytes)
        decoded = Expr.from_bytes(blob)
        assert decoded.canonical_name() == e.canonical_name()

    def test_explicit_ctx_used(self, ctx):
        e = col("a") + lit(1)
        decoded = Expr.from_bytes(e.to_bytes(), ctx=ctx)
        assert decoded.canonical_name() == e.canonical_name()


class TestUDFCodec:
    """Python scalar UDFs ride inside the proto blob via the Rust codec.

    No worker context needed on the receiver — the cloudpickled callable is
    embedded in ``fun_definition`` and reconstructed automatically.
    """

    def test_udf_self_contained_blob(self):
        e = _double_udf()(col("a"))
        blob = pickle.dumps(e)
        # The codec inlines the callable, so the blob is much bigger than a
        # pure built-in blob but doesn't depend on receiver-side registration.
        assert len(blob) > 200

    def test_udf_decodes_into_fresh_ctx(self):
        e = _double_udf()(col("a"))
        blob = e.to_bytes()
        fresh = SessionContext()
        decoded = Expr.from_bytes(blob, ctx=fresh)
        assert "double" in decoded.canonical_name()

    def test_udf_decodes_via_pickle_with_no_worker_ctx(self):
        e = _double_udf()(col("a"))
        blob = pickle.dumps(e)
        decoded = pickle.loads(blob)  # noqa: S301
        assert "double" in decoded.canonical_name()

    def test_udf_decodes_via_pickle_with_worker_ctx(self):
        set_worker_ctx(SessionContext())
        e = _double_udf()(col("a"))
        blob = pickle.dumps(e)
        decoded = pickle.loads(blob)  # noqa: S301
        assert "double" in decoded.canonical_name()

    def test_closure_capturing_udf_names_match(self):
        captured_multiplier = 7

        def fn(arr):
            return pa.array([(v.as_py() or 0) * captured_multiplier for v in arr])

        u = udf(
            fn,
            [pa.int64()],
            pa.int64(),
            volatility="immutable",
            name="times_seven",
        )
        e = u(col("a"))
        blob = pickle.dumps(e)
        decoded = pickle.loads(blob)  # noqa: S301
        assert decoded.canonical_name() == e.canonical_name()

    def test_multi_arg_udf_round_trip(self):
        """Wire format builds synthetic `arg_{i}` fields per input — exercise
        with a 2-arg UDF spanning two distinct DataTypes."""
        add_scaled = udf(
            lambda a, b: pa.array(
                [
                    (x.as_py() or 0) + (y.as_py() or 0.0)
                    for x, y in zip(a, b, strict=False)
                ]
            ),
            [pa.int64(), pa.float64()],
            pa.float64(),
            volatility="immutable",
            name="add_scaled",
        )
        e = add_scaled(col("a"), col("b"))
        decoded = pickle.loads(pickle.dumps(e))  # noqa: S301
        assert decoded.canonical_name() == e.canonical_name()
        assert "add_scaled" in decoded.canonical_name()


class TestAggregateUDFCodec:
    """Python aggregate UDFs travel inline like scalar UDFs."""

    def _build_aggregate_udf(self):
        from datafusion import udaf
        from datafusion.user_defined import Accumulator

        class CountAcc(Accumulator):
            def __init__(self):
                self._count = 0

            def state(self):
                return [pa.scalar(self._count, type=pa.int64())]

            def update(self, values):
                self._count += len(values)

            def merge(self, states):
                for s in states:
                    self._count += s[0].as_py()

            def evaluate(self):
                return pa.scalar(self._count, type=pa.int64())

        return udaf(
            CountAcc,
            [pa.int64()],
            pa.int64(),
            [pa.int64()],
            "immutable",
            name="count_all",
        )

    def test_agg_udf_self_contained_blob(self):
        u = self._build_aggregate_udf()
        e = u(col("a"))
        blob = pickle.dumps(e)
        assert len(blob) > 200

    def test_agg_udf_decodes_into_fresh_ctx(self):
        u = self._build_aggregate_udf()
        e = u(col("a"))
        blob = e.to_bytes()
        fresh = SessionContext()
        decoded = Expr.from_bytes(blob, ctx=fresh)
        assert "count_all" in decoded.canonical_name()

    def test_agg_udf_decodes_via_pickle_with_no_worker_ctx(self):
        u = self._build_aggregate_udf()
        e = u(col("a"))
        blob = pickle.dumps(e)
        decoded = pickle.loads(blob)  # noqa: S301
        assert "count_all" in decoded.canonical_name()

    def test_agg_udf_evaluates_after_roundtrip(self):
        """End-to-end: the decoded aggregate UDF runs and merges across
        partitions, exercising the round-tripped state-field schema."""
        u = self._build_aggregate_udf()
        e = u(col("a"))
        decoded = pickle.loads(pickle.dumps(e))  # noqa: S301

        ctx = SessionContext()
        df = ctx.from_pydict({"a": [1, 2, 3, 4, 5]})
        out = df.aggregate([], [decoded.alias("n")]).to_pydict()
        assert out["n"] == [5]


class TestWindowUDFCodec:
    """Python window UDFs travel inline like scalar UDFs."""

    def _build_window_udf(self):
        from datafusion import udwf
        from datafusion.user_defined import WindowEvaluator

        class CountUpEvaluator(WindowEvaluator):
            def evaluate_all(self, values, num_rows):
                return pa.array(list(range(num_rows)))

        return udwf(
            CountUpEvaluator,
            [pa.int64()],
            pa.int64(),
            "immutable",
            name="count_up",
        )

    def test_window_udf_self_contained_blob(self):
        u = self._build_window_udf()
        e = u(col("a"))
        blob = pickle.dumps(e)
        assert len(blob) > 200

    def test_window_udf_decodes_into_fresh_ctx(self):
        u = self._build_window_udf()
        e = u(col("a"))
        blob = e.to_bytes()
        fresh = SessionContext()
        decoded = Expr.from_bytes(blob, ctx=fresh)
        assert "count_up" in decoded.canonical_name()

    def test_window_udf_decodes_via_pickle_with_no_worker_ctx(self):
        u = self._build_window_udf()
        e = u(col("a"))
        blob = pickle.dumps(e)
        decoded = pickle.loads(blob)  # noqa: S301
        assert "count_up" in decoded.canonical_name()

    def test_window_udf_evaluates_after_roundtrip(self):
        """End-to-end: decoded window UDF runs and emits per-row values
        produced by the round-tripped evaluator factory."""
        from datafusion.expr import WindowFrame

        u = self._build_window_udf()
        e = u(col("a"))
        decoded = pickle.loads(pickle.dumps(e))  # noqa: S301

        ctx = SessionContext()
        df = ctx.from_pydict({"a": [1, 2, 3, 4, 5]})
        framed = (
            decoded.window_frame(WindowFrame("rows", None, None)).build().alias("c")
        )
        out = df.select(framed).to_pydict()
        assert out["c"] == [0, 1, 2, 3, 4]


class TestErrorPaths:
    def test_from_bytes_rejects_garbage(self):
        with pytest.raises(Exception):  # noqa: B017
            Expr.from_bytes(b"not a valid protobuf payload")

    def test_from_bytes_rejects_empty(self):
        with pytest.raises(Exception):  # noqa: B017
            Expr.from_bytes(b"")

    def test_cross_version_error_message(self):
        """Decoding a payload stamped with a different Python minor
        version raises a clear, actionable error rather than an opaque
        marshal/unpickle failure.

        The wire frame inside the protobuf is:
        ``DFPYUDF (7) | version (1) | py_major (1) | py_minor (1) | cloudpickle``.
        We locate the frame inside the outer protobuf and patch the
        minor byte at offset 9.
        """
        import sys

        e = _double_udf()(col("a"))
        blob = e.to_bytes()

        idx = blob.find(b"DFPYUDF")
        assert idx >= 0, "DFPYUDF frame not found in payload"

        different_minor = (sys.version_info.minor + 1) % 256
        tampered = bytearray(blob)
        tampered[idx + 9] = different_minor

        with pytest.raises(
            Exception, match="not portable across Python minor versions"
        ):
            Expr.from_bytes(bytes(tampered))
