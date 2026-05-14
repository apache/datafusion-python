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

from __future__ import annotations

import pytest
from datafusion import ExecutionPlan
from datafusion_ffi_example import MyExecutionPlan


@pytest.mark.parametrize("inner_capsule", [True, False])
def test_execution_plan_from_pycapsule(inner_capsule: bool) -> None:
    """`ExecutionPlan.from_pycapsule` consumes a capsule sourced from
    a downstream Rust crate, regardless of whether the user passes the
    typed wrapper or pre-extracts the raw capsule.

    `MyExecutionPlan` produces a real `DataSourceExec` backed by a
    `MemorySourceConfig`. After the FFI handoff the underlying plan
    is wrapped in a `ForeignExecutionPlan`, so the display surface
    shows the FFI envelope plus the wrapped plan's name — schema
    detail is not propagated by the FFI display path.
    """
    obj = MyExecutionPlan(num_rows=5)
    if inner_capsule:
        obj = obj.__datafusion_execution_plan__()

    plan = ExecutionPlan.from_pycapsule(obj)

    assert plan.children() == []
    assert plan.partition_count == 1
    assert "DataSourceExec" in plan.display()


# `to_bytes` round-trip of an FFI-imported `ExecutionPlan` requires a
# physical codec that knows how to encode `ForeignExecutionPlan`. The
# default codec does not, so a downstream user serializing such a plan
# must install their own physical extension codec via
# `SessionContext.with_physical_extension_codec(...)`. We do not
# exercise that here because the test would assert the encode path on
# the user's codec, not on datafusion-python's plumbing.
