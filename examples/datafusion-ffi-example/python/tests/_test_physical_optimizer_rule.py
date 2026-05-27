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

import pyarrow as pa
from datafusion import SessionContext
from datafusion_ffi_example import MyPhysicalOptimizerRule


def _setup_session_with_rule() -> tuple[SessionContext, MyPhysicalOptimizerRule]:
    rule = MyPhysicalOptimizerRule()
    ctx = SessionContext(physical_optimizer_rules=[rule])
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3])],
        names=["a"],
    )
    ctx.register_record_batches("t", [[batch]])
    return ctx, rule


def test_ffi_physical_optimizer_rule_runs_during_planning():
    """A rule supplied via physical_optimizer_rules is invoked while the
    physical plan is built, and the query still returns correct results."""
    ctx, rule = _setup_session_with_rule()

    before = rule.optimize_calls()
    result = ctx.sql("SELECT a FROM t").collect()
    after = rule.optimize_calls()

    assert after > before, (
        f"Expected user FFI physical optimizer rule to fire, "
        f"before={before} after={after}"
    )
    assert result[0].column(0).to_pylist() == [1, 2, 3]


def test_ffi_physical_optimizer_rule_export():
    """The rule object exposes the FFI capsule entry point."""
    rule = MyPhysicalOptimizerRule()
    capsule = rule.__datafusion_physical_optimizer_rule__()
    assert capsule is not None
