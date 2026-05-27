.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. _execution_metrics:

Execution Metrics
=================

Overview
--------

When DataFusion executes a query it compiles the logical plan into a tree of
*physical plan operators* (e.g. ``FilterExec``, ``ProjectionExec``,
``HashAggregateExec``). Each operator can record runtime statistics while it
runs. These statistics are called **execution metrics**.

Typical metrics include:

- **output_rows** – number of rows produced by the operator
- **elapsed_compute** – total CPU time (nanoseconds) spent inside the operator
- **spill_count** – number of times the operator spilled data to disk
- **spilled_bytes** – total bytes written to disk during spills
- **spilled_rows** – total rows written to disk during spills

Metrics are collected *per-partition*: DataFusion may execute each operator
in parallel across several partitions. The convenience properties on
:py:class:`~datafusion.MetricsSet` (e.g. ``output_rows``, ``elapsed_compute``)
automatically sum the named metric across **all** partitions, giving a single
aggregate value for the operator as a whole. You can also access the raw
per-partition :py:class:`~datafusion.Metric` objects via
:py:meth:`~datafusion.MetricsSet.metrics`.

When Are Metrics Available?
---------------------------

Some operators (for example ``DataSourceExec``) eagerly create a
:py:class:`~datafusion.MetricsSet` when the physical plan is built, so
:py:meth:`~datafusion.ExecutionPlan.metrics` may return a set even before any
rows have been processed.  However, metric **values** such as ``output_rows``
are only meaningful **after** the DataFrame has been executed via one of the
terminal operations:

- :py:meth:`~datafusion.DataFrame.collect`
- :py:meth:`~datafusion.DataFrame.collect_partitioned`
- :py:meth:`~datafusion.DataFrame.execute_stream`
  (metrics are available once the stream has been fully consumed)
- :py:meth:`~datafusion.DataFrame.execute_stream_partitioned`
  (metrics are available once all partition streams have been fully consumed)

Before execution, metric values will be ``0`` or ``None``.

.. note::

   **display() does not populate metrics.**
   When a DataFrame is displayed in a notebook (e.g. via ``display(df)`` or
   automatic ``repr`` output), DataFusion runs a *limited* internal execution
   to fetch preview rows.  This internal execution does **not** cache the
   physical plan used, so :py:meth:`~datafusion.ExecutionPlan.collect_metrics`
   will not reflect the display execution.  To access metrics you must call
   one of the terminal operations listed above.

If you call :py:meth:`~datafusion.DataFrame.collect` (or another terminal
operation) multiple times on the same DataFrame, each call creates a fresh
physical plan.  Metrics from :py:meth:`~datafusion.DataFrame.execution_plan`
always reflect the **most recent** execution.

Reading the Physical Plan Tree
--------------------------------

:py:meth:`~datafusion.DataFrame.execution_plan` returns the root
:py:class:`~datafusion.ExecutionPlan` node of the physical plan tree. The tree
mirrors the operator pipeline: the root is typically a projection or
coalescing node; its children are filters, aggregates, scans, etc.

The ``operator_name`` string returned by
:py:meth:`~datafusion.ExecutionPlan.collect_metrics` is the *display* name of
the node, for example ``"FilterExec: column1@0 > 1"``. This is the same string
you would see when calling ``plan.display()``.

Aggregated vs Per-Partition Metrics
------------------------------------

DataFusion executes each operator across one or more **partitions** in
parallel.  The :py:class:`~datafusion.MetricsSet` convenience properties
(``output_rows``, ``elapsed_compute``, etc.) automatically **sum** the named
metric across all partitions, giving a single aggregate value.

To inspect individual partitions — for example to detect data skew where one
partition processes far more rows than others — iterate over the raw
:py:class:`~datafusion.Metric` objects:

.. code-block:: python

    for metric in metrics_set.metrics():
        print(f"  partition={metric.partition}  {metric.name}={metric.value}")

The ``partition`` property is a 0-based index (``0``, ``1``, …) identifying
which parallel slot processed this metric.  It is ``None`` for metrics that
apply globally (not tied to a specific partition).

Available Metrics
-----------------

The following metrics are directly accessible as properties on
:py:class:`~datafusion.MetricsSet`:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Property
     - Description
   * - ``output_rows``
     - Number of rows emitted by the operator (summed across partitions).
   * - ``elapsed_compute``
     - Wall-clock CPU time **in nanoseconds** spent inside the operator's
       compute loop, excluding I/O wait.  Useful for identifying which
       operators are most expensive (summed across partitions).
   * - ``spill_count``
     - Number of spill-to-disk events triggered by memory pressure.  This is
       a unitless count of events, not a measure of data volume (summed across
       partitions).
   * - ``spilled_bytes``
     - Total bytes written to disk during spill events (summed across
       partitions).
   * - ``spilled_rows``
     - Total rows written to disk during spill events (summed across
       partitions).

Any metric not listed above can be accessed via
:py:meth:`~datafusion.MetricsSet.sum_by_name`, or by iterating over the raw
:py:class:`~datafusion.Metric` objects returned by
:py:meth:`~datafusion.MetricsSet.metrics`.

Labels
------

A :py:class:`~datafusion.Metric` may carry *labels*: key/value pairs that
provide additional context.  Labels are operator-specific; most metrics have
an empty label dict.

Some operators tag their metrics with labels to distinguish variants.  For
example, a ``HashAggregateExec`` may record separate ``output_rows`` metrics
for intermediate and final output:

.. code-block:: python

    for metric in metrics_set.metrics():
        print(metric.name, metric.labels())
    # output_rows  {'output_type': 'final'}
    # output_rows  {'output_type': 'intermediate'}

When summing by name (via :py:attr:`~datafusion.MetricsSet.output_rows` or
:py:meth:`~datafusion.MetricsSet.sum_by_name`), **all** metrics with that
name are summed regardless of labels.  To filter by label, iterate over the
raw :py:class:`~datafusion.Metric` objects directly.

End-to-End Example
------------------

.. code-block:: python

    from datafusion import SessionContext

    ctx = SessionContext()
    ctx.sql("CREATE TABLE sales AS VALUES (1, 100), (2, 200), (3, 50)")

    df = ctx.sql("SELECT * FROM sales WHERE column1 > 1")

    # Execute the query — this populates the metrics
    results = df.collect()

    # Retrieve the physical plan with metrics
    plan = df.execution_plan()

    # Walk every operator and print its metrics
    for operator_name, ms in plan.collect_metrics():
        if ms.output_rows is not None:
            print(f"{operator_name}")
            print(f"  output_rows    = {ms.output_rows}")
            print(f"  elapsed_compute = {ms.elapsed_compute} ns")

    # Access raw per-partition metrics
    for operator_name, ms in plan.collect_metrics():
        for metric in ms.metrics():
            print(
                f"  partition={metric.partition}  "
                f"{metric.name}={metric.value}  "
                f"labels={metric.labels()}"
            )

API Reference
-------------

- :py:class:`datafusion.ExecutionPlan` — physical plan node
- :py:meth:`datafusion.ExecutionPlan.collect_metrics` — walk the tree and
  return ``(operator_name, MetricsSet)`` pairs
- :py:meth:`datafusion.ExecutionPlan.metrics` — return the
  :py:class:`~datafusion.MetricsSet` for a single node
- :py:class:`datafusion.MetricsSet` — aggregated metrics for one operator
- :py:class:`datafusion.Metric` — a single per-partition metric value
