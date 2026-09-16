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
from datafusion_ffi_example import MyFunctionExtension


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
