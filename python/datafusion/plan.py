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
    import datetime

    from datafusion.context import SessionContext

__all__ = [
    "ExecutionPlan",
    "LogicalPlan",
    "Metric",
    "MetricsSet",
]


class LogicalPlan:  # noqa: PLW1641
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
        """Return metrics for this plan node, or None if this plan has no MetricsSet.

        Some operators (e.g. DataSourceExec) eagerly initialize a MetricsSet
        when the plan is created, so this may return a set even before
        execution.  Metric *values* (such as ``output_rows``) are only
        meaningful after the DataFrame has been executed.
        """
        raw = self._raw_plan.metrics()
        if raw is None:
            return None
        return MetricsSet(raw)

    def collect_metrics(self) -> list[tuple[str, MetricsSet]]:
        """Return runtime statistics for each step of the query execution.

        DataFusion executes a query as a pipeline of operators — for example a
        data source scan, followed by a filter, followed by a projection. After
        the DataFrame has been executed (via
        :py:meth:`~datafusion.DataFrame.collect`,
        :py:meth:`~datafusion.DataFrame.execute_stream`, etc.), each operator
        records statistics such as how many rows it produced and how much CPU
        time it consumed.

        Each entry in the returned list corresponds to one operator that
        recorded metrics. The first element of the tuple is the operator's
        description string — the same text shown by
        :py:meth:`display_indent` — which identifies both the operator type
        and its key parameters, for example ``"FilterExec: column1@0 > 1"``
        or ``"DataSourceExec: partitions=1"``.

        Returns:
            A list of ``(description, MetricsSet)`` tuples ordered from the
            outermost operator (top of the execution tree) down to the
            data-source leaves. Only operators that recorded at least one
            metric are included. Returns an empty list if called before the
            DataFrame has been executed.
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

    A physical plan operator runs independently across one or more partitions.
    :py:meth:`metrics` returns the raw per-partition :py:class:`Metric` objects.
    The convenience properties (:py:attr:`output_rows`, :py:attr:`elapsed_compute`,
    etc.) automatically sum the named metric across *all* partitions, giving a
    single aggregate value for the operator as a whole.
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
        """Total CPU time (in nanoseconds) spent inside this operator's execute loop.

        Summed across all partitions. Returns ``None`` if no ``elapsed_compute``
        metric was recorded.
        """
        return self._raw.elapsed_compute()

    @property
    def spill_count(self) -> int | None:
        """Number of times this operator spilled data to disk due to memory pressure.

        This is a count of spill events, not a byte count. Summed across all
        partitions. Returns ``None`` if no ``spill_count`` metric was recorded.
        """
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
        """Sum the named metric across all partitions.

        Useful for accessing any metric not exposed as a first-class property.
        Returns ``None`` if no metric with the given name was recorded.

        Args:
            name: The metric name, e.g. ``"output_rows"`` or ``"elapsed_compute"``.
        """
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
    def value(self) -> int | datetime.datetime | None:
        """The value of this metric.

        Returns an ``int`` for counters, gauges, and time-based metrics
        (nanoseconds), a :py:class:`~datetime.datetime` (UTC) for
        ``start_timestamp`` / ``end_timestamp`` metrics, or ``None``
        when the value has not been set or is not representable.
        """
        return self._raw.value

    @property
    def value_as_datetime(self) -> datetime.datetime | None:
        """The value as a UTC :py:class:`~datetime.datetime` for timestamp metrics.

        Returns ``None`` for all non-timestamp metrics and for timestamp
        metrics whose value has not been set (e.g. before execution).
        """
        return self._raw.value_as_datetime

    @property
    def partition(self) -> int | None:
        """The 0-based partition index this metric applies to.

        Returns ``None`` for metrics that are not partition-specific (i.e. they
        apply globally across all partitions of the operator).
        """
        return self._raw.partition

    def labels(self) -> dict[str, str]:
        """Return the labels associated with this metric.

        Labels provide additional context for a metric.  For example::

            metric.labels()
            # {'output_type': 'final'}
        """
        return self._raw.labels()

    def __repr__(self) -> str:
        """Return a string representation of the metric."""
        return repr(self._raw)
