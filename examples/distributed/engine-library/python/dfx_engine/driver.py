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
scheduling. What matters is the four things the driver has to get right, each
of which is a way a real deployment goes wrong:

1. It serializes the stage **with** its session. ``to_bytes(None)`` uses an
   empty codec chain and cannot encode any library's node.
2. It ships the codec ids it used, so a worker can refuse a plan it would
   misread rather than decode it with the wrong codec.
3. It puts the shuffle directory in the session config, not in the message,
   so the directory travels *inside* the encoded plan and the two sides
   cannot disagree.
4. It waits for every worker before reading, because the stage node decides
   whether to read or recompute by looking at the filesystem.
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

__all__ = ["DistributedResult", "find_stage", "run_distributed"]


class DistributedResult:
    """What a distributed run produced, and how."""

    def __init__(
        self,
        batches: list[pa.RecordBatch],
        partitions: list[int],
        worker_rows: dict[int, int],
    ) -> None:
        self.batches = batches
        self.partitions = partitions
        """Partition indices that were dispatched, one per worker."""
        self.worker_rows = worker_rows
        """Rows each worker produced, keyed by partition index."""


STAGE_NODE_NAME = "ShuffleStageExec"


def find_stage(plan: ExecutionPlan) -> ExecutionPlan | None:
    """Locate the stage node the planner inserted.

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
    is involved.
    """
    if STAGE_NODE_NAME in plan.display():
        return plan
    for child in plan.children():
        found = find_stage(child)
        if found is not None:
            return found
    return None


def _dispatch(
    envelope: dict, envelope_dir: pathlib.Path, partition: int
) -> subprocess.Popen[str]:
    """Start one worker for one partition.

    ``sys.executable``, not ``python``: a worker on a different Python minor
    version cannot load a cloudpickled inline UDF, and that failure is far
    from its cause.
    """
    path = envelope_dir / f"task-{partition}.json"
    path.write_text(json.dumps(envelope))
    return subprocess.Popen(  # noqa: S603
        [sys.executable, "-m", "dfx_engine.worker", str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def run_distributed(
    sql: str, spec: SessionSpec, extra_udfs: list[ScalarUDF] | None = None
) -> DistributedResult:
    """Run `sql`, executing its leaf stage in one worker process per partition.

    Requires ``spec.shuffle_dir``: without it the planner inserts no stage and
    there is nothing to distribute.

    ``extra_udfs`` are registered on the driver only. They have to be here for
    the query to *plan*, but not on the worker: a Python UDF is cloudpickled
    into the plan and travels by value, unlike the Rust functions in
    :func:`~dfx_engine.session.build_session`, which travel by name and so
    have to exist on both sides.
    """
    if not spec.shuffle_dir:
        message = "run_distributed needs a shuffle_dir; build_session got none"
        raise ValueError(message)

    ctx, engine, _storage = build_session(spec)
    for function in extra_udfs or []:
        ctx.register_udf(function)
    plan = ctx.sql(sql).execution_plan()

    stage = find_stage(plan)
    if stage is None:
        message = (
            "no ShuffleStageExec in the plan; the engine's planner did not run, "
            "or its config extension was not registered"
        )
        raise RuntimeError(message)

    shuffle_dir = pathlib.Path(spec.shuffle_dir)
    shuffle_dir.mkdir(parents=True, exist_ok=True)

    # Encode the stage subtree, through the session that owns the codecs.
    plan_path = shuffle_dir / "stage.plan"
    plan_path.write_bytes(stage.to_bytes(ctx))

    stage_id = _internal.stage_id()
    partitions = list(range(stage.partition_count))
    envelopes = [
        {
            "spec": spec.to_json(),
            "plan": str(plan_path),
            "stage_id": stage_id,
            "partition": partition,
        }
        for partition in partitions
    ]

    # One process per partition, all in flight together. This is the claim the
    # example is making: each worker reads a different file and writes a
    # different result, so they need no coordination beyond the directory.
    workers = [
        _dispatch(envelope, shuffle_dir, partition)
        for envelope, partition in zip(envelopes, partitions, strict=True)
    ]

    worker_rows: dict[int, int] = {}
    failures = []
    for partition, worker in zip(partitions, workers, strict=True):
        stdout, stderr = worker.communicate()
        if worker.returncode != 0:
            failures.append(f"partition {partition} failed:\n{stderr}")
            continue
        worker_rows[partition] = json.loads(stdout)["rows"]

    if failures:
        raise RuntimeError("\n".join(failures))

    # Now run the whole query here. Every stage partition has a file, so the
    # stage node streams them instead of recomputing -- the driver does only
    # the final merge.
    batches = ctx.sql(sql).collect()
    _ = engine
    return DistributedResult(batches, partitions, worker_rows)


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
