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

import pytest
from datafusion import SessionContext
from datafusion.optimizer import AnalyzerRule, OptimizerRule
from datafusion.plan import LogicalPlan


def test_optimizer_rule_is_invoked_during_planning() -> None:
    """A registered OptimizerRule is called as the plan is optimized."""
    seen_plans: list[str] = []

    class TracingRule(OptimizerRule):
        def name(self) -> str:
            return "tracing_rule"

        def rewrite(self, plan: LogicalPlan) -> LogicalPlan | None:
            seen_plans.append(plan.display())
            return None

    ctx = SessionContext()
    ctx.add_optimizer_rule(TracingRule())
    df = ctx.from_pydict({"a": [1, 2, 3]})
    result = df.collect()

    # The rule sees the plan at least once during optimization, and
    # since it returns None each time the optimizer terminates cleanly.
    assert seen_plans, "optimizer rule was not invoked during planning"
    assert result[0].column(0).to_pylist() == [1, 2, 3]


def test_optimizer_rule_can_be_removed_by_name() -> None:
    """remove_optimizer_rule deregisters a user-supplied rule by name."""

    class NoopRule(OptimizerRule):
        def name(self) -> str:
            return "noop_for_removal"

        def rewrite(self, plan: LogicalPlan) -> LogicalPlan | None:
            return None

    ctx = SessionContext()
    ctx.add_optimizer_rule(NoopRule())
    assert ctx.remove_optimizer_rule("noop_for_removal") is True
    # Second remove returns False — already gone.
    assert ctx.remove_optimizer_rule("noop_for_removal") is False


def test_analyzer_rule_is_invoked_during_analysis() -> None:
    """A registered AnalyzerRule is called and must return a plan."""
    invocations: list[str] = []

    class IdentityAnalyzer(AnalyzerRule):
        def name(self) -> str:
            return "identity_analyzer"

        def analyze(self, plan: LogicalPlan) -> LogicalPlan:
            invocations.append(plan.display())
            return plan

    ctx = SessionContext()
    ctx.add_analyzer_rule(IdentityAnalyzer())
    df = ctx.from_pydict({"a": [1, 2, 3]})
    result = df.collect()

    assert invocations, "analyzer rule was not invoked"
    assert result[0].column(0).to_pylist() == [1, 2, 3]


def test_analyzer_rule_returning_none_errors() -> None:
    """Analyzer rules must return a LogicalPlan; None surfaces as an error."""

    class BadAnalyzer(AnalyzerRule):
        def name(self) -> str:
            return "bad_analyzer"

        def analyze(self, plan: LogicalPlan):  # type: ignore[override]
            return None

    ctx = SessionContext()
    ctx.add_analyzer_rule(BadAnalyzer())
    df = ctx.from_pydict({"a": [1]})
    with pytest.raises(Exception, match="bad_analyzer"):
        df.collect()


def test_optimizer_rule_abc_cannot_be_instantiated() -> None:
    """OptimizerRule is abstract — direct instantiation must fail."""
    with pytest.raises(TypeError):
        OptimizerRule()  # type: ignore[abstract]


def test_analyzer_rule_abc_cannot_be_instantiated() -> None:
    """AnalyzerRule is abstract — direct instantiation must fail."""
    with pytest.raises(TypeError):
        AnalyzerRule()  # type: ignore[abstract]
