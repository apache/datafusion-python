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
    """`ExecutionPlan.from_pycapsule` consumes a capsule sourced from a
    non-datafusion-python crate, regardless of whether the user passes
    the typed wrapper or pre-extracts the raw capsule."""
    obj = MyExecutionPlan()
    if inner_capsule:
        obj = obj.__datafusion_execution_plan__()

    plan = ExecutionPlan.from_pycapsule(obj)

    # EmptyExec round-trips with no children.
    assert plan.children() == []
    assert plan.partition_count == 1
    assert "EmptyExec" in plan.display()
