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

"""The driver: split a query into tasks, fan them out, collect the answer.

The shape is deliberately boring, because the interesting part is not the
scheduling. What matters is the six things the driver has to get right, each
of which is a way a real deployment goes wrong:

1. It serializes each stage **with** its session. ``to_bytes(None)`` uses an
   empty codec chain and cannot encode any library's node.
2. It ships the codec ids it used, so a worker can refuse a plan it would
   misread rather than decode it with the wrong codec.
3. It puts the shuffle directory in the session config, not in the message,
   so the directory travels *inside* the encoded plan and the two sides
   cannot disagree.
4. It waits for every worker before reading, because the stage node decides
   whether to read or recompute by looking at the filesystem.
5. It ships *every* stage. A plan can hold more than one -- an aggregate in
   each branch of a union, say -- and they are independent subtrees rather
   than a chain.
6. It refuses a shuffle directory that already holds stage output. That same
   "read it if it is there" rule is what makes a *second* query in the same
   directory read the first one's results -- see :func:`require_empty_shuffle`.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys
from typing import TYPE_CHECKING

from dfx_engine import _internal
from dfx_engine.session import SessionSpec, build_session

if TYPE_CHECKING:
    import pyarrow as pa
    from datafusion import DataFrame, SessionContext
    from datafusion.plan import ExecutionPlan
    from datafusion.user_defined import ScalarUDF

__all__ = [
    "DistributedResult",
    "dataframe_for",
    "find_stage",
    "find_stages",
    "require_empty_shuffle",
    "run_distributed",
    "run_local",
]


class DistributedResult:
    """What a distributed run produced, and how."""

    def __init__(
        self,
        batches: list[pa.RecordBatch],
        tasks: list[tuple[int, int]],
        task_rows: dict[tuple[int, int], int],
    ) -> None:
        self.batches = batches
        self.tasks = tasks
        """``(stage_id, partition)`` pairs that were dispatched, one per worker.

        Keyed by both, not by partition alone: a query with an aggregate in
        more than one branch has more than one stage, and partition 0 of each
        is a different piece of work.
        """
        self.task_rows = task_rows
        """Rows each worker produced, keyed as :attr:`tasks` is."""


STAGE_NODE_NAME = "ShuffleStageExec"


def find_stages(plan: ExecutionPlan) -> list[ExecutionPlan]:
    """Locate every stage node the planner inserted, in pre-order.

    Matched on the display string because a Python caller has no way to
    downcast a Rust plan node -- there is no ``isinstance`` across an FFI
    boundary.

    Note the *containment* test. The node was built inside this library and
    handed back to the host, so what the host prints is not
    ``ShuffleStageExec: stage=1`` but::

        FFI_ExecutionPlan: ShuffleStageExec, number_of_children=1

    A foreign node reports its own name nested inside the wrapper's, which
    makes anchored matches on plan text quietly wrong -- the kind of thing
    that works in a single-library test and fails the moment a real extension
    is involved. It is also why the stage *id* has to be recomputed here
    rather than read: the wrapper dropped it.

    Pre-order is the contract with the planner, which numbers stages in the
    same walk, so the nth node returned here has the id
    ``_internal.stage_id(n)``. The recursion stops at a stage rather than
    descending into it, because the planner never nests one inside another.
    """
    if STAGE_NODE_NAME in plan.display():
        return [plan]
    return [stage for child in plan.children() for stage in find_stages(child)]


def find_stage(plan: ExecutionPlan) -> ExecutionPlan | None:
    """The first stage in `plan`, or ``None``.

    For tests and callers that only care whether the planner split at all.
    See :func:`find_stages`.
    """
    stages = find_stages(plan)
    return stages[0] if stages else None


def _dispatch(
    envelope: dict, envelope_dir: pathlib.Path, stage_id: int, partition: int
) -> subprocess.Popen[str]:
    """Start one worker for one ``(stage, partition)``.

    ``sys.executable``, not ``python``: a worker on a different Python minor
    version cannot load a cloudpickled inline UDF, and that failure is far
    from its cause.
    """
    path = envelope_dir / f"task-{stage_id}-{partition}.json"
    path.write_text(json.dumps(envelope))
    return subprocess.Popen(  # noqa: S603
        [sys.executable, "-m", "dfx_engine.worker", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _report(task: tuple[int, int], stdout: str) -> int:
    """Read a worker's row count off its last line of output.

    The last line, not the whole stream: a worker's stdout is shared with
    everything loaded into it, and one stray `print` from a library -- or a
    warning some future dependency decides to write there -- would turn
    `json.loads` on the whole buffer into a confusing failure a long way from
    its cause.
    """
    stage_id, partition = task
    lines = [line for line in stdout.splitlines() if line.strip()]
    if not lines:
        message = f"stage {stage_id} partition {partition} printed no report"
        raise RuntimeError(message)
    try:
        return json.loads(lines[-1])["rows"]
    except (ValueError, KeyError) as err:
        message = (
            f"stage {stage_id} partition {partition} printed an unreadable "
            f"report {lines[-1]!r}"
        )
        raise RuntimeError(message) from err


def require_empty_shuffle(shuffle_dir: pathlib.Path) -> None:
    """Refuse a directory that already holds stage output.

    A stage node reads partition `i` if the file for it exists and computes it
    otherwise, which is what lets one node be both halves of the exchange. The
    cost is that the filesystem *is* the state, and stage ids restart at
    ``stage_id(0)`` for every plan -- so a second query in the same directory
    finds files left by the first and reads them, having computed nothing.

    Nothing downstream can catch that. If the two plans' stage schemas differ
    the failure is an unrelated-looking schema error a long way from here, and
    if they agree -- the same query over data that has since changed, say --
    the answer is simply the old one, silently.

    So the rule this enforces is **one shuffle directory per query**, and the
    check belongs here rather than in the node: the node cannot tell a file
    this run's worker wrote from one last run's worker wrote, but the driver
    knows it has not dispatched anything yet.

    Scoped to stage output, not to "is the directory empty". The two are the
    same thing only because :func:`run_distributed` keeps its own scratch in
    a ``tasks/`` subdirectory; a caller who points this at a directory of
    their own should get an answer about the files that would actually be
    read, not about the ones they put there.
    """
    stale = sorted(path.name for path in shuffle_dir.glob(_internal.partition_glob()))
    if stale:
        message = (
            f"{shuffle_dir} already holds stage output {stale}; a stage reads "
            f"a partition file if it finds one, so this query would return the "
            f"previous query's results. Use one shuffle directory per query."
        )
        raise RuntimeError(message)


def run_distributed(
    sql: str, spec: SessionSpec, extra_udfs: list[ScalarUDF] | None = None
) -> DistributedResult:
    """Run `sql`, executing each stage partition in its own worker process.

    Requires ``spec.shuffle_dir``: without it the planner inserts no stage and
    there is nothing to distribute. The directory must not already hold stage
    output -- see :func:`require_empty_shuffle`.

    ``extra_udfs`` are registered on the driver only. They have to be here for
    the query to *plan*, but not on the worker: a Python UDF is cloudpickled
    into the plan and travels by value, unlike the Rust functions in
    :func:`~dfx_engine.session.build_session`, which travel by name and so
    have to exist on both sides.
    """
    if not spec.shuffle_dir:
        message = "run_distributed needs a shuffle_dir; build_session got none"
        raise ValueError(message)

    # Before building a session, so a reused directory costs nothing to
    # diagnose. `glob` on a directory that does not exist yet yields nothing.
    require_empty_shuffle(pathlib.Path(spec.shuffle_dir))

    ctx, _engine, _storage = build_session(spec)
    for function in extra_udfs or []:
        ctx.register_udf(function)
    plan = ctx.sql(sql).execution_plan()

    stages = find_stages(plan)
    if not stages:
        message = (
            "no ShuffleStageExec in the plan; the engine's planner did not run, "
            "or its config extension was not registered"
        )
        raise RuntimeError(message)

    shuffle_dir = pathlib.Path(spec.shuffle_dir)
    # The driver's scratch lives in a subdirectory rather than beside the
    # results. The stage node owns the `stage-*-part-*.arrow` namespace in
    # `shuffle_dir` and nothing else should write there, so that
    # :func:`require_empty_shuffle` is asking about stage output rather than
    # about whatever else the driver happens to have left lying around.
    task_dir = shuffle_dir / "tasks"
    task_dir.mkdir(parents=True, exist_ok=True)

    # One task per (stage, partition). Every stage is shipped, not just the
    # first: a query with an aggregate in two branches has two independent
    # subtrees, and leaving one behind would have the driver compute it
    # locally while the plan it shipped claimed otherwise.
    tasks: list[tuple[int, int]] = []
    envelopes = []
    for index, stage in enumerate(stages):
        # The id the planner gave this stage, recomputed from its position
        # because the FFI wrapper's display does not carry it.
        stage_id = _internal.stage_id(index)
        # Encode the stage subtree, through the session that owns the codecs.
        plan_path = task_dir / f"stage-{stage_id}.plan"
        plan_path.write_bytes(stage.to_bytes(ctx))
        for partition in range(stage.partition_count):
            tasks.append((stage_id, partition))
            envelopes.append(
                {
                    "spec": spec.to_json(),
                    "plan": str(plan_path),
                    "stage_id": stage_id,
                    "partition": partition,
                }
            )

    # One process per task, all in flight together. This is the claim the
    # example is making: each worker reads a different file and writes a
    # different result, so they need no coordination beyond the directory.
    workers = [
        _dispatch(envelope, task_dir, stage_id, partition)
        for envelope, (stage_id, partition) in zip(envelopes, tasks, strict=True)
    ]

    task_rows: dict[tuple[int, int], int] = {}
    failures = []
    for task, worker in zip(tasks, workers, strict=True):
        stdout, stderr = worker.communicate()
        if worker.returncode != 0:
            stage_id, partition = task
            failures.append(f"stage {stage_id} partition {partition} failed:\n{stderr}")
            continue
        task_rows[task] = _report(task, stdout)

    if failures:
        raise RuntimeError("\n".join(failures))

    # Now run the whole query here. Every stage partition has a file, so the
    # stage nodes stream them instead of recomputing -- the driver does only
    # the final merge.
    batches = ctx.sql(sql).collect()
    return DistributedResult(batches, tasks, task_rows)


def run_local(
    sql: str, spec: SessionSpec, extra_udfs: list[ScalarUDF] | None = None
) -> list[pa.RecordBatch]:
    """Run `sql` in this process, for comparison.

    Uses the same session factory with no shuffle directory, so the only
    difference from :func:`run_distributed` is where the work happened. Any
    disagreement between the two is a bug in the split.
    """
    ctx, _engine, _storage = build_session(
        SessionSpec(
            tables=spec.tables,
            shuffle_dir="",
            target_partitions=spec.target_partitions,
        )
    )
    for function in extra_udfs or []:
        ctx.register_udf(function)
    return ctx.sql(sql).collect()


def dataframe_for(sql: str, spec: SessionSpec) -> tuple[SessionContext, DataFrame]:
    """Session and DataFrame for `sql`, for tests that want to inspect a plan."""
    ctx, _engine, _storage = build_session(spec)
    return ctx, ctx.sql(sql)
