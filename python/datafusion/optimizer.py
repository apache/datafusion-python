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

"""Abstract base classes for user-defined optimizer and analyzer rules.

DataFusion's planner is built from two pipelines:

* The :class:`Analyzer <AnalyzerRule>` runs first and is responsible for
  semantic rewrites — type coercion, function lookup, and rewrites that
  cannot leave the plan structurally unchanged. Analyzer rules must
  return a fully rewritten :class:`~datafusion.plan.LogicalPlan` every
  time they run.
* The :class:`Optimizer <OptimizerRule>` runs afterwards and applies
  cost-driven or semantics-preserving transformations until a fixed
  point is reached. Optimizer rules may return ``None`` to signal "no
  change," letting the optimizer terminate as soon as no rule mutates
  the plan.

Both ABCs are registered against a :class:`~datafusion.SessionContext`
through :py:meth:`~datafusion.SessionContext.add_optimizer_rule` /
:py:meth:`~datafusion.SessionContext.add_analyzer_rule`.

The upstream rule traits also receive an
``OptimizerConfig`` / ``ConfigOptions`` reference. Those are not
surfaced to Python here; rules that need configuration access should
capture state at construction time (for example a
:class:`~datafusion.SessionContext` reference) or be implemented in
Rust.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion.plan import LogicalPlan

__all__ = ["AnalyzerRule", "OptimizerRule"]


class OptimizerRule(ABC):
    """Abstract base class for a user-defined optimizer rule.

    Subclasses must implement :py:meth:`name` and :py:meth:`rewrite`.

    Examples:
        >>> import datafusion as dfn
        >>> from datafusion.optimizer import OptimizerRule
        >>> from datafusion.plan import LogicalPlan
        >>>
        >>> class TaggingRule(OptimizerRule):
        ...     # Mark each plan we see; never actually mutate it.
        ...     def __init__(self) -> None:
        ...         self.seen = 0
        ...
        ...     def name(self) -> str:
        ...         return "tagging_rule"
        ...
        ...     def rewrite(self, plan: LogicalPlan) -> LogicalPlan | None:
        ...         self.seen += 1
        ...         return None
        >>>
        >>> ctx = dfn.SessionContext()
        >>> rule = TaggingRule()
        >>> ctx.add_optimizer_rule(rule)
        >>> ctx.from_pydict({"a": [1]}).count()
        1
        >>> rule.seen > 0
        True
    """

    @abstractmethod
    def name(self) -> str:
        """Return a unique name for this rule.

        DataFusion uses the name to deduplicate rules and to support
        removal via :py:meth:`~datafusion.SessionContext.remove_optimizer_rule`.
        """

    @abstractmethod
    def rewrite(self, plan: LogicalPlan) -> LogicalPlan | None:
        """Attempt to rewrite ``plan``.

        Return a new :class:`~datafusion.plan.LogicalPlan` if the rule
        produced one, or ``None`` to indicate no change. The optimizer
        calls each rule repeatedly until no rule reports a change, so
        returning ``None`` when nothing was rewritten is important for
        termination.
        """


class AnalyzerRule(ABC):
    """Abstract base class for a user-defined analyzer rule.

    Subclasses must implement :py:meth:`name` and :py:meth:`analyze`.
    Unlike optimizer rules, analyzer rules must always return a
    :class:`~datafusion.plan.LogicalPlan` (return the input plan
    unmodified when nothing applies).

    Examples:
        >>> import datafusion as dfn
        >>> from datafusion.optimizer import AnalyzerRule
        >>> from datafusion.plan import LogicalPlan
        >>>
        >>> class IdentityAnalyzer(AnalyzerRule):
        ...     def name(self) -> str:
        ...         return "identity_analyzer"
        ...
        ...     def analyze(self, plan: LogicalPlan) -> LogicalPlan:
        ...         return plan
        >>>
        >>> ctx = dfn.SessionContext()
        >>> ctx.add_analyzer_rule(IdentityAnalyzer())
        >>> ctx.from_pydict({"a": [1, 2, 3]}).count()
        3
    """

    @abstractmethod
    def name(self) -> str:
        """Return a unique name for this rule."""

    @abstractmethod
    def analyze(self, plan: LogicalPlan) -> LogicalPlan:
        """Rewrite ``plan`` and return the new plan.

        Analyzer rules must always return a
        :class:`~datafusion.plan.LogicalPlan`. Return the input plan
        unchanged when there is nothing to rewrite.
        """
