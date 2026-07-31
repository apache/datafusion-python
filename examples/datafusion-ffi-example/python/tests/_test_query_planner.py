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
from datafusion import SessionContext
from datafusion_ffi_example import MyQueryPlanner


@pytest.mark.parametrize("raw_capsule", [False, True])
def test_ffi_query_planner_runs_during_planning(raw_capsule: bool):
    """A query planner imported from another library creates the physical plan."""
    planner = MyQueryPlanner()
    exported_planner = (
        planner.__datafusion_query_planner__() if raw_capsule else planner
    )
    ctx = SessionContext().with_query_planner(exported_planner)

    before = planner.plan_calls()
    result = ctx.sql("SELECT 1 AS value").collect()
    after = planner.plan_calls()

    assert after > before
    assert result == []
