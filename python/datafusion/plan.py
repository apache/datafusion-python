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

"""This module supports physical and logical plans in DataFusion."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import datafusion._internal as df_internal

if TYPE_CHECKING:
    from datafusion.context import SessionContext

__all__ = [
    "ExecutionPlan",
    "LogicalPlan",
    "Metric",
    "MetricsSet",
]


class LogicalPlan:
    """Logical Plan.

    A `LogicalPlan` is a node in a tree of relational operators (such as
    Projection or Filter).

    Represents transforming an input relation (table) to an output relation
    (table) with a potentially different schema. Plans form a dataflow tree
    where data flows from leaves up to the root to produce the query result.

    A `LogicalPlan` can be created by the SQL query planner, the DataFrame API,
    or programmatically (for example custom query languages).
    """

    def __init__(self, plan: df_internal.LogicalPlan) -> None:
        """This constructor should not be called by the end user."""
        self._raw_plan = plan

    def to_variant(self) -> Any:
        """Convert the logical plan into its specific variant."""
        return self._raw_plan.to_variant()

    def inputs(self) -> list[LogicalPlan]:
        """Returns the list of inputs to the logical plan."""
        return [LogicalPlan(p) for p in self._raw_plan.inputs()]

    def __repr__(self) -> str:
        """Generate a printable representation of the plan."""
        return self._raw_plan.__repr__()

    def display(self) -> str:
        """Print the logical plan."""
        return self._raw_plan.display()

    def display_indent(self) -> str:
        """Print an indented form of the logical plan."""
        return self._raw_plan.display_indent()

    def display_indent_schema(self) -> str:
        """Print an indented form of the schema for the logical plan."""
        return self._raw_plan.display_indent_schema()

    def display_graphviz(self) -> str:
        """Print the graph visualization of the logical plan.

        Returns a `format`able structure that produces lines meant for graphical display
        using the `DOT` language. This format can be visualized using software from
        [`graphviz`](https://graphviz.org/)
        """
        return self._raw_plan.display_graphviz()

    @staticmethod
    def from_proto(ctx: SessionContext, data: bytes) -> LogicalPlan:
        """Create a LogicalPlan from protobuf bytes.

        Tables created in memory from record batches are currently not supported.
        """
        return LogicalPlan(df_internal.LogicalPlan.from_proto(ctx.ctx, data))

    def to_proto(self) -> bytes:
        """Convert a LogicalPlan to protobuf bytes.

        Tables created in memory from record batches are currently not supported.
        """
        return self._raw_plan.to_proto()

    def __eq__(self, other: LogicalPlan) -> bool:
        """Test equality."""
        if not isinstance(other, LogicalPlan):
            return False
        return self._raw_plan.__eq__(other._raw_plan)


class ExecutionPlan:
    """Represent nodes in the DataFusion Physical Plan."""

    def __init__(self, plan: df_internal.ExecutionPlan) -> None:
        """This constructor should not be called by the end user."""
        self._raw_plan = plan

    def children(self) -> list[ExecutionPlan]:
        """Get a list of children `ExecutionPlan` that act as inputs to this plan.

        The returned list will be empty for leaf nodes such as scans, will contain a
        single value for unary nodes, or two values for binary nodes (such as joins).
        """
        return [ExecutionPlan(e) for e in self._raw_plan.children()]

    def display(self) -> str:
        """Print the physical plan."""
        return self._raw_plan.display()

    def display_indent(self) -> str:
        """Print an indented form of the physical plan."""
        return self._raw_plan.display_indent()

    def __repr__(self) -> str:
        """Print a string representation of the physical plan."""
        return self._raw_plan.__repr__()

    @property
    def partition_count(self) -> int:
        """Returns the number of partitions in the physical plan."""
        return self._raw_plan.partition_count

    @staticmethod
    def from_proto(ctx: SessionContext, data: bytes) -> ExecutionPlan:
        """Create an ExecutionPlan from protobuf bytes.

        Tables created in memory from record batches are currently not supported.
        """
        return ExecutionPlan(df_internal.ExecutionPlan.from_proto(ctx.ctx, data))

    def to_proto(self) -> bytes:
        """Convert an ExecutionPlan into protobuf bytes.

        Tables created in memory from record batches are currently not supported.
        """
        return self._raw_plan.to_proto()

    def metrics(self) -> MetricsSet | None:
        """Return metrics for this plan node after execution, or None if unavailable."""
        raw = self._raw_plan.metrics()
        if raw is None:
            return None
        return MetricsSet(raw)

    def collect_metrics(self) -> list[tuple[str, MetricsSet]]:
        """Walk the plan tree and collect metrics from all operators.

        Returns a list of (operator_name, MetricsSet) tuples.
        """
        result: list[tuple[str, MetricsSet]] = []

        def _walk(node: ExecutionPlan) -> None:
            ms = node.metrics()
            if ms is not None:
                result.append((node.display(), ms))
            for child in node.children():
                _walk(child)

        _walk(self)
        return result


class MetricsSet:
    """A set of metrics for a single execution plan operator.

    Provides both individual metric access and convenience aggregations
    across partitions.
    """

    def __init__(self, raw: df_internal.MetricsSet) -> None:
        """This constructor should not be called by the end user."""
        self._raw = raw

    def metrics(self) -> list[Metric]:
        """Return all individual metrics in this set."""
        return [Metric(m) for m in self._raw.metrics()]

    @property
    def output_rows(self) -> int | None:
        """Sum of output_rows across all partitions."""
        return self._raw.output_rows()

    @property
    def elapsed_compute(self) -> int | None:
        """Sum of elapsed_compute across all partitions, in nanoseconds."""
        return self._raw.elapsed_compute()

    @property
    def spill_count(self) -> int | None:
        """Sum of spill_count across all partitions."""
        return self._raw.spill_count()

    @property
    def spilled_bytes(self) -> int | None:
        """Sum of spilled_bytes across all partitions."""
        return self._raw.spilled_bytes()

    @property
    def spilled_rows(self) -> int | None:
        """Sum of spilled_rows across all partitions."""
        return self._raw.spilled_rows()

    def sum_by_name(self, name: str) -> int | None:
        """Return the sum of metrics matching the given name."""
        return self._raw.sum_by_name(name)

    def __repr__(self) -> str:
        """Return a string representation of the metrics set."""
        return repr(self._raw)


class Metric:
    """A single execution metric with name, value, partition, and labels."""

    def __init__(self, raw: df_internal.Metric) -> None:
        """This constructor should not be called by the end user."""
        self._raw = raw

    @property
    def name(self) -> str:
        """The name of this metric (e.g. ``output_rows``)."""
        return self._raw.name

    @property
    def value(self) -> int | None:
        """The numeric value of this metric, or None for non-numeric types."""
        return self._raw.value

    @property
    def partition(self) -> int | None:
        """The partition this metric applies to, or None if global."""
        return self._raw.partition

    def labels(self) -> dict[str, str]:
        """Return the labels associated with this metric."""
        return self._raw.labels()

    def __repr__(self) -> str:
        """Return a string representation of the metric."""
        return repr(self._raw)
