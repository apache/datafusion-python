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

"""What a function library gets for shipping a bundle instead of a recipe."""

from __future__ import annotations

import pyarrow as pa
import pytest
from datafusion import SessionContext, SessionExtensionComponents
from datafusion_ffi_example import MyFunctionExtension, MyRuleExtension


def _session():
    """A session with the library installed in one call.

    The comparison this file exists to make: without a bundle this is three
    ``register_*`` calls the caller has to know about, one per function.
    """
    ctx = SessionContext().with_extensions(MyFunctionExtension())
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3, None])], names=["a"])
    ctx.register_record_batches("test_table", [[batch]])
    return ctx


def test_one_call_installs_every_function():
    """All three kinds arrive across the FFI boundary from one hook."""
    ctx = _session()

    scalar = ctx.sql("select my_custom_is_null(a) from test_table").collect()
    assert [r.column(0) for r in scalar] == [
        pa.array([False, False, False, True], type=pa.bool_())
    ]

    aggregate = ctx.sql("select my_custom_sum(a) from test_table").collect()
    assert aggregate[0].column(0)[0].as_py() == 6

    window = ctx.sql(
        "select my_custom_rank() over (order by a) from test_table"
    ).collect()
    assert window[0].num_rows == 4


def test_the_names_come_from_the_capsules():
    """Not from anything the bundle or the host said.

    The wrappers are built by the host during resolution, so a name it invented
    would be the one a query had to use. These are the names the Rust
    ``ScalarUDFImpl`` and friends report.
    """
    ctx = _session()

    assert ctx.udf("my_custom_is_null").name == "my_custom_is_null"
    assert ctx.udaf("my_custom_sum").name == "my_custom_sum"
    assert ctx.udwf("my_custom_rank").name == "my_custom_rank"


def test_the_bundle_is_reusable_across_sessions():
    """One bundle object, two sessions: components are built per install."""
    extension = MyFunctionExtension()
    first = SessionContext().with_extensions(extension)
    second = SessionContext().with_extensions(extension)

    assert first.udf("my_custom_is_null").name == "my_custom_is_null"
    assert second.udf("my_custom_is_null").name == "my_custom_is_null"
    assert first.session_id() != second.session_id()


def test_installing_the_library_twice_is_refused():
    """The collision rule holds for functions arriving over FFI.

    Two instances of one library is the shape this actually takes in the wild —
    an application assembling its extension list from a plugin registry that
    lists the same package twice.
    """
    ctx = SessionContext()

    with pytest.raises(ValueError, match=r"scalar function named 'my_custom_is_null'"):
        ctx.with_extensions(MyFunctionExtension(), MyFunctionExtension())

    with pytest.raises(KeyError):
        ctx.udf("my_custom_is_null")


def test_a_failure_after_the_hook_registers_nothing():
    """The transaction covers functions imported across the FFI boundary too."""
    ctx = SessionContext()

    class BoomPlanner:
        def __datafusion_session_planner__(self, ctx, fallback) -> None:
            msg = "boom"
            raise RuntimeError(msg)

    with pytest.raises(RuntimeError, match="boom"):
        ctx.with_extensions(MyFunctionExtension(), BoomPlanner())

    with pytest.raises(KeyError):
        ctx.udf("my_custom_is_null")


def _query(ctx):
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3])], names=["a"])
    ctx.register_record_batches("t", [[batch]])
    return ctx.sql("SELECT a FROM t").collect()


def test_declared_rules_all_fire():
    """Rules accumulate, so both of a bundle's two rules run.

    Nothing about installing the second displaces the first, which is what
    makes rules different from a planner and why there is no collision to
    refuse.
    """
    extension = MyRuleExtension()
    ctx = SessionContext().with_extensions(extension)

    assert _query(ctx)[0].column(0).to_pylist() == [1, 2, 3]
    assert extension.first_calls() > 0
    assert extension.second_calls() > 0


def test_declared_rules_run_in_declaration_order():
    """The order a bundle lists its rules in is the order they install in.

    Rules rewrite the plan one after another, so the order is part of what a
    bundle declares. The counters cannot show it — each rule has its own — so
    the two here append to a log they share.
    """
    extension = MyRuleExtension()
    ctx = SessionContext().with_extensions(extension)

    _query(ctx)

    assert extension.run_order() == [0, 1]


def test_rules_install_without_changing_the_session_id():
    """Installing rules rebuilds ``SessionState``; the id has to survive it.

    A fresh id would leave ``session_id()`` disagreeing with every
    ``TaskContext`` the session already handed out, which is exactly what a
    codec's decode callbacks resolve against.
    """
    ctx = SessionContext()
    before = ctx.session_id()
    result = ctx.with_extensions(MyRuleExtension())

    assert result.session_id() == before
    assert ctx.session_id() == before


def test_rules_and_functions_install_together():
    """Two bundles, one contributing functions and one rules, in one call."""
    rules = MyRuleExtension()
    ctx = SessionContext().with_extensions(MyFunctionExtension(), rules)
    batch = pa.RecordBatch.from_arrays([pa.array([1, 2, None])], names=["a"])
    ctx.register_record_batches("t", [[batch]])

    result = ctx.sql("SELECT my_custom_is_null(a) FROM t").collect()

    assert result[0].column(0).to_pylist() == [False, False, True]
    assert rules.first_calls() > 0


def test_a_failure_leaves_no_rule_installed():
    """The transaction covers rules, which write through a state rebuild.

    A rule reaching the session before the failing hook would be invisible to
    ``session_id()`` and to the function registry, so this asserts on the
    counter instead: an installed rule fires on the next query.
    """
    rules = MyRuleExtension()
    ctx = SessionContext()

    class BoomPlanner:
        def __datafusion_session_planner__(self, ctx, fallback) -> None:
            msg = "boom"
            raise RuntimeError(msg)

    with pytest.raises(RuntimeError, match="boom"):
        ctx.with_extensions(rules, BoomPlanner())

    _query(ctx)
    assert rules.first_calls() == 0
    assert rules.second_calls() == 0


def test_the_hook_returns_the_components_type():
    """The bundle builds a real dataclass, not a duck-typed stand-in.

    ``with_extensions`` rejects anything else, so a Rust bundle that imported
    the wrong name would fail at install rather than silently contribute
    nothing.
    """
    components = MyFunctionExtension().__datafusion_session_components__(
        SessionContext()
    )

    assert isinstance(components, SessionExtensionComponents)
    assert len(components.udfs) == 1
    assert components.logical_extension_codecs == ()
