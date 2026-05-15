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

Cross-process tests live in ``test_pickle_multiprocessing.py``.
"""

from __future__ import annotations

import pickle
import threading

import pyarrow as pa
import pytest
from datafusion import Expr, SessionContext, col, lit, udf
from datafusion.ipc import clear_worker_ctx, get_worker_ctx, set_worker_ctx


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
        decoded = pickle.loads(blob)
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
        decoded = pickle.loads(blob)
        assert "double" in decoded.canonical_name()

    def test_udf_decodes_via_pickle_with_worker_ctx(self):
        set_worker_ctx(SessionContext())
        e = _double_udf()(col("a"))
        blob = pickle.dumps(e)
        decoded = pickle.loads(blob)
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
        decoded = pickle.loads(blob)
        # Round-trip names match; functional verification of captured state
        # happens in test_pickle_multiprocessing via an actual UDF call.
        assert decoded.canonical_name() == e.canonical_name()


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
        from datafusion import Expr

        decoded = Expr.from_bytes(blob, ctx=fresh)
        assert "count_all" in decoded.canonical_name()

    def test_agg_udf_decodes_via_pickle_with_no_worker_ctx(self):
        u = self._build_aggregate_udf()
        e = u(col("a"))
        blob = pickle.dumps(e)
        decoded = pickle.loads(blob)
        assert "count_all" in decoded.canonical_name()


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
        from datafusion import Expr

        decoded = Expr.from_bytes(blob, ctx=fresh)
        assert "count_up" in decoded.canonical_name()

    def test_window_udf_decodes_via_pickle_with_no_worker_ctx(self):
        u = self._build_window_udf()
        e = u(col("a"))
        blob = pickle.dumps(e)
        decoded = pickle.loads(blob)
        assert "count_up" in decoded.canonical_name()


class TestPythonUdfInliningToggle:
    """`SessionContext.with_python_udf_inlining(False)` opts out of
    inline Python UDF encoding for both encode and decode paths."""

    def _build_double_udf(self):
        return udf(
            lambda arr: pa.array([(v.as_py() or 0) * 2 for v in arr]),
            [pa.int64()],
            pa.int64(),
            volatility="immutable",
            name="double",
        )

    def test_strict_encoder_emits_smaller_blob(self):
        """Strict mode skips cloudpickle of the Python callable, so the
        encoded bytes are dramatically smaller than the inline form."""
        ctx_inline = SessionContext()
        ctx_strict = ctx_inline.with_python_udf_inlining(False)
        u = self._build_double_udf()
        e = u(col("a"))

        blob_inline = e.to_bytes(ctx_inline)
        blob_strict = e.to_bytes(ctx_strict)

        assert len(blob_strict) < len(blob_inline) // 4

    def test_strict_roundtrip_via_registry(self):
        """When both sender and receiver disable inlining, the UDF
        travels by name only and the receiver resolves it from its
        registered functions."""
        from datafusion import Expr

        strict_sender = SessionContext().with_python_udf_inlining(False)
        u = self._build_double_udf()
        blob = u(col("a")).to_bytes(strict_sender)

        receiver = SessionContext().with_python_udf_inlining(False)
        receiver.register_udf(u)
        restored = Expr.from_bytes(blob, ctx=receiver)
        assert "double" in restored.canonical_name()

    def test_strict_decoder_refuses_inline_payload(self):
        """An inline-encoded blob fed to a strict receiver raises with a
        clear error rather than silently invoking cloudpickle.loads."""
        from datafusion import Expr

        sender = SessionContext()
        u = self._build_double_udf()
        blob = u(col("a")).to_bytes(sender)

        strict_receiver = SessionContext().with_python_udf_inlining(False)
        strict_receiver.register_udf(u)
        with pytest.raises(Exception, match="inlining is disabled"):
            Expr.from_bytes(blob, ctx=strict_receiver)


class TestWorkerCtxLifecycle:
    def test_set_and_clear(self):
        assert get_worker_ctx() is None
        ctx = SessionContext()
        set_worker_ctx(ctx)
        assert get_worker_ctx() is ctx
        clear_worker_ctx()
        assert get_worker_ctx() is None

    def test_clear_when_unset_is_noop(self):
        clear_worker_ctx()  # no error
        assert get_worker_ctx() is None

    def test_thread_local_isolation(self):
        main_ctx = SessionContext()
        set_worker_ctx(main_ctx)

        seen_in_thread: list = []

        def worker():
            seen_in_thread.append(get_worker_ctx())
            set_worker_ctx(SessionContext())
            seen_in_thread.append(get_worker_ctx())

        t = threading.Thread(target=worker)
        t.start()
        t.join()

        # Thread saw no ctx initially (thread-local), then its own.
        assert seen_in_thread[0] is None
        assert seen_in_thread[1] is not main_ctx
        # Main thread's ctx is unchanged by the thread's actions.
        assert get_worker_ctx() is main_ctx
