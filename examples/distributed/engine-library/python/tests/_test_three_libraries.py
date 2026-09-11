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

"""Three separately-compiled libraries, one distributed query.

Every test here runs real worker processes. The comparison that matters is
between the distributed answer and the single-process answer through the same
session factory: if those ever disagree, the split is wrong.
"""

from __future__ import annotations

import dataclasses
import pathlib
import re

import cloudpickle
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from datafusion import SessionContext, udf
from dfx_engine import _internal
from dfx_engine.driver import find_stage, find_stages, run_distributed, run_local
from dfx_engine.session import SessionSpec, build_session, expected_codec_ids
from dfx_engine.worker import run_task

Q1 = """
select l_returnflag, l_linestatus,
       count(*) as n,
       sum(l_quantity) as qty,
       sum(l_extendedprice) as price
from lineitem
group by l_returnflag, l_linestatus
order by l_returnflag, l_linestatus
"""

REVENUE = """
select l_returnflag,
       sum(dfx_net_revenue(l_extendedprice, l_discount, l_tax)) as revenue,
       dfx_weighted_avg(l_extendedprice, l_quantity) as wavg
from lineitem
group by l_returnflag
order by l_returnflag
"""


def _module_level_bucket(prices: pa.Array) -> pa.Array:
    """A UDF body at module scope, for the by-reference test.

    Defined here rather than inside the test on purpose: a function with a
    resolvable ``module.qualname`` is pickled as a pointer to it, and this
    module is not importable from a worker.
    """
    return pc.if_else(pc.greater(prices, 400.0), pa.scalar("high"), pa.scalar("low"))


def _rows(batches: list[pa.RecordBatch]) -> list[tuple]:
    table = pa.Table.from_batches(batches) if batches else None
    if table is None:
        return []
    columns = [table.column(i).to_pylist() for i in range(table.num_columns)]
    return list(zip(*columns, strict=True))


# --- the four queries -------------------------------------------------------


def test_distributed_aggregate_matches_single_process(spec: SessionSpec) -> None:
    """Query 1: the baseline. Four input files, four workers, one answer."""
    result = run_distributed(Q1, spec)

    # One stage, so every task is a partition of stage 1.
    assert result.tasks == [(1, 0), (1, 1), (1, 2), (1, 3)]
    assert _rows(result.batches) == _rows(run_local(Q1, spec))
    # Checked by hand against the fixture.
    assert _rows(result.batches) == [
        ("A", "F", 3, 10.0, 1000.0),
        ("N", "O", 3, 15.0, 1500.0),
        ("R", "F", 2, 11.0, 1100.0),
    ]


def test_a_rust_udf_resolves_on_every_worker(spec: SessionSpec) -> None:
    """Query 2: functions from a library that ships no bundle.

    `dfx_udfs` is installed by hand in `build_session`, and the aggregate runs
    partially on each worker and finally on the driver -- so its two-sum state
    has to survive the split.
    """
    result = run_distributed(REVENUE, spec)

    assert _rows(result.batches) == _rows(run_local(REVENUE, spec))
    revenue = {row[0]: row[1] for row in _rows(result.batches)}
    # Flag A sums three rows: full price, plus tax, then half off.
    assert revenue["A"] == pytest.approx(730.0)
    # Flag R sums two: one discounted a fifth, one taxed a fifth.
    assert revenue["R"] == pytest.approx(1160.0)


def test_an_inline_python_udf_ships_by_value(spec: SessionSpec) -> None:
    """Query 3: a Python callable defined right here, running on a worker.

    Defined *inside* the test function, so its qualified name is
    ``...<locals>.bucket`` and cloudpickle cannot look it up -- which means it
    travels by value, bytecode and all. The worker has never imported this
    file and does not need to.
    """

    def bucket(prices: pa.Array) -> pa.Array:
        # `pc` is a module, and cloudpickle resolves it to `pyarrow.compute`
        # and stores an import of it -- so the worker imports the submodule on
        # load and this works. Modules are the easy case; see
        # `test_a_by_reference_capture_fails_on_the_worker` for the hard one.
        return pc.if_else(
            pc.greater(prices, 400.0), pa.scalar("high"), pa.scalar("low")
        )

    price_bucket = udf(
        bucket, [pa.float64()], pa.string(), volatility="immutable", name="price_bucket"
    )

    sql = """
    select price_bucket(l_extendedprice) as bucket, count(*) as n
    from lineitem group by bucket order by bucket
    """

    ctx, _engine, _storage = build_session(spec)
    ctx.register_udf(price_bucket)
    plan = ctx.sql(sql).execution_plan()
    stage = find_stage(plan)
    assert stage is not None
    # The callable itself is in the bytes, under the scalar-UDF family prefix.
    assert b"DFPYUDF" in stage.to_bytes(ctx)

    result = run_distributed(sql, spec, extra_udfs=[price_bucket])
    # Prices run from one hundred to eight hundred, so four exceed four hundred.
    assert _rows(result.batches) == [("high", 4), ("low", 4)]
    assert _rows(result.batches) == _rows(
        run_local(sql, spec, extra_udfs=[price_bucket])
    )


def test_a_by_reference_capture_fails_on_the_worker(spec: SessionSpec) -> None:
    """The pitfall half of query 3, and the reason to read this file.

    The callable's *body* travels by value. Names it closes over travel by
    **reference** if cloudpickle can find them under an importable module --
    and this test file is an importable module, so `_module_level_bucket` is
    stored as a two-word pointer at it.

    The driver runs the query fine: the name resolves here. The worker has
    never heard of this module and fails on import, with an error that names
    the module and says nothing about UDFs, plans, or serialization.

    A module is the easy case, because the worker can just import it (see the
    previous test). A *function in your own project* is the case that bites:
    it means every worker needs your code installed, not just your data.
    """
    price_bucket = udf(
        _module_level_bucket,
        [pa.float64()],
        pa.string(),
        volatility="immutable",
        name="price_bucket",
    )
    sql = "select price_bucket(l_extendedprice) as bucket from lineitem"

    # Pinning *why* it breaks: a reference, not a copy. The by-value version
    # in the previous test is two orders of magnitude bigger.
    assert len(cloudpickle.dumps(_module_level_bucket)) < 200
    assert b"_test_three_libraries" in cloudpickle.dumps(_module_level_bucket)

    # Works here, because the name resolves in this process.
    assert len(run_local(sql, spec, extra_udfs=[price_bucket])) >= 1

    with pytest.raises(RuntimeError) as excinfo:
        run_distributed(sql, spec, extra_udfs=[price_bucket])
    assert "_test_three_libraries" in str(excinfo.value)


def test_the_custom_provider_is_read_on_the_workers(spec: SessionSpec) -> None:
    """Query 4: the storage library's scan, executed in another process.

    Its codec had to write the directory into the logical plan *and* the file
    list into the physical plan for this to work at all.
    """
    sql = "select count(*) as n, sum(l_quantity) as qty from lineitem"
    result = run_distributed(sql, spec)

    assert _rows(result.batches) == [(8, 36.0)]
    assert _rows(result.batches) == _rows(run_local(sql, spec))


# --- what the split actually did --------------------------------------------


def test_every_partition_ran_exactly_once(spec: SessionSpec) -> None:
    """Each worker got a different partition, and together they covered it."""
    result = run_distributed(Q1, spec)

    assert sorted(result.tasks) == [(1, 0), (1, 1), (1, 2), (1, 3)]
    assert len(set(result.tasks)) == len(result.tasks)
    # Two rows per input file, so each worker saw two rows' worth of groups.
    assert sum(result.task_rows.values()) == 8
    assert set(result.task_rows) == set(result.tasks)


def test_each_worker_published_its_own_file(spec: SessionSpec) -> None:
    """One shuffle file per partition, and nothing left half-written."""
    run_distributed(Q1, spec)

    shuffle = pathlib.Path(spec.shuffle_dir)
    produced = sorted(path.name for path in shuffle.glob("*.arrow"))
    expected = sorted(
        pathlib.Path(
            _internal.partition_path(spec.shuffle_dir, _internal.stage_id(), partition)
        ).name
        for partition in range(4)
    )
    assert produced == expected
    # Nothing half-written: a temporary file is unique to its writer and is
    # renamed away when the partition is complete.
    assert list(shuffle.glob("*.tmp")) == []


def test_the_driver_reads_the_workers_output(spec: SessionSpec) -> None:
    """Corrupt one shuffle file and the driver's query breaks.

    Without this the suite could not tell a distributed run from the driver
    quietly recomputing everything and getting the same answer.
    """
    run_distributed(Q1, spec)

    victim = pathlib.Path(
        _internal.partition_path(spec.shuffle_dir, _internal.stage_id(), 1)
    )
    victim.write_bytes(b"not an arrow stream")

    ctx, _engine, _storage = build_session(spec)
    with pytest.raises(Exception, match="dfx_engine: reading"):
        ctx.sql(Q1).collect()


def test_each_librarys_codec_carried_its_own_node(spec: SessionSpec) -> None:
    """Both codecs installed is not the same as both codecs used."""
    ctx, engine, storage = build_session(spec)
    plan = ctx.sql(Q1).execution_plan()
    stage = find_stage(plan)
    assert stage is not None

    # Already one apiece, before the driver has asked for any bytes: an FFI
    # query planner returns its plan as protobuf rather than as a handle, so
    # every query serializes the planner's output on the way back. Worth
    # knowing before reading an encode counter as "this is what shipping
    # cost".
    assert engine.encode_calls() == 1
    assert storage.encode_calls() == 1

    stage.to_bytes(ctx)

    # Now once more each, this time because the driver asked.
    assert engine.encode_calls() == 2
    assert storage.encode_calls() == 2


def test_the_plan_splits_at_the_partial_aggregate(spec: SessionSpec) -> None:
    """The stage boundary is where the aggregate already splits itself."""
    ctx, engine, _storage = build_session(spec)
    plan = ctx.sql(Q1).execution_plan()

    text = plan.display_indent()
    assert "mode=FinalPartitioned" in text
    assert "ShuffleStageExec" in text
    # The final aggregate is above the stage, the partial one inside it.
    assert text.index("FinalPartitioned") < text.index("ShuffleStageExec")
    assert text.index("ShuffleStageExec") < text.index("mode=Partial")
    assert engine.stages_inserted() == 1

    stage = find_stage(plan)
    assert stage is not None
    # One stage partition per input file, which is what makes the fan-out
    # meaningful rather than a single remote call.
    assert stage.partition_count == 4
    assert stage.output_partitioning.scheme == "UnknownPartitioning"


TWO_BRANCHES = """
select l_returnflag as g, sum(l_quantity) as qty
from lineitem group by l_returnflag
union all
select l_linestatus as g, sum(l_quantity) as qty
from other group by l_linestatus
"""


def test_two_branches_get_two_separately_numbered_stages(
    lineitem_dir: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """A partial aggregate in each branch is two stages, not one.

    Both branches are independent subtrees and both want shipping, so the
    planner wraps each. They must not share a stage id: a stage exchanges
    results through paths built from that id, so two stages numbered alike
    would write to the same files -- and because a union drives its branches
    concurrently, they would do it at the same time. That fails outright
    rather than quietly, but only because the schemas happen to differ.
    """
    spec = SessionSpec(
        tables={"lineitem": str(lineitem_dir), "other": str(lineitem_dir)},
        shuffle_dir=str(tmp_path / "shuffle"),
        target_partitions=2,
    )

    ctx, engine, _storage = build_session(spec)
    plan = ctx.sql(TWO_BRANCHES).execution_plan()

    assert plan.display_indent().count("ShuffleStageExec") == 2
    assert len(find_stages(plan)) == 2
    assert engine.stages_inserted() == 2

    result = run_distributed(TWO_BRANCHES, spec)

    # Four partitions apiece, under two distinct stage ids.
    assert sorted(result.tasks) == [
        (stage, part) for stage in (1, 2) for part in range(4)
    ]
    # Sorted: a union has no ordering of its own, so the branches interleave
    # differently from run to run.
    assert sorted(_rows(result.batches)) == sorted(_rows(run_local(TWO_BRANCHES, spec)))

    # Each stage published its own files, so neither read the other's.
    produced = sorted(
        path.name for path in pathlib.Path(spec.shuffle_dir).glob("*.arrow")
    )
    assert produced == sorted(
        pathlib.Path(
            _internal.partition_path(spec.shuffle_dir, _internal.stage_id(index), part)
        ).name
        for index in range(2)
        for part in range(4)
    )


# --- the ways it goes wrong -------------------------------------------------


def test_without_a_shuffle_dir_nothing_is_distributed(
    lineitem_dir: pathlib.Path,
) -> None:
    """The engine declines to insert a stage it has nowhere to put.

    Better than inserting one and failing at execute time, and it is what
    makes `run_local` use the same factory as the distributed path.
    """
    local = SessionSpec(tables={"lineitem": str(lineitem_dir)}, shuffle_dir="")
    ctx, engine, _storage = build_session(local)

    plan = ctx.sql(Q1).execution_plan()
    assert find_stage(plan) is None
    assert engine.plan_calls() >= 1
    assert engine.stages_inserted() == 0
    # And the query still answers correctly, in this process.
    assert _rows(ctx.sql(Q1).collect())[0] == ("A", "F", 3, 10.0, 1000.0)


def test_a_reused_shuffle_directory_is_refused(
    spec: SessionSpec, tmp_path: pathlib.Path
) -> None:
    """A second query in one directory would read the first one's results.

    Stage ids restart at 1 for every plan and a stage reads a partition file
    if it finds one, so the files `Q1` leaves behind are exactly the files
    `REVENUE`'s stage looks for. Nothing downstream can catch that: here the
    two schemas differ so it would surface as an unrelated-looking error, but
    re-running *the same* query over changed data would simply return the old
    answer.
    """
    run_distributed(Q1, spec)

    with pytest.raises(RuntimeError, match="already holds stage output") as excinfo:
        run_distributed(REVENUE, spec)

    # Names the files and the rule, so the reader does not have to work out
    # why a directory that "looks fine" was rejected.
    assert "stage-1-part-0.arrow" in str(excinfo.value)
    assert "one shuffle directory per query" in str(excinfo.value)

    # And a fresh directory is all it takes.
    elsewhere = dataclasses.replace(spec, shuffle_dir=str(tmp_path / "second"))
    assert _rows(run_distributed(REVENUE, elsewhere).batches) == _rows(
        run_local(REVENUE, spec)
    )


def test_the_guard_is_scoped_to_stage_output(spec: SessionSpec) -> None:
    """The driver's own scratch in the same directory is not stage output.

    `run_distributed` writes each stage's encoded plan and each worker's task
    envelope beside the results, so a check that rejected any non-empty
    directory would reject every second call for the wrong reason -- and the
    obvious fix, deleting what it found, would delete those too.
    """
    shuffle = pathlib.Path(spec.shuffle_dir)
    shuffle.mkdir(parents=True)
    (shuffle / "stage-1.plan").write_bytes(b"leftover")
    (shuffle / "task-1-0.json").write_text("{}")

    result = run_distributed(Q1, spec)

    assert _rows(result.batches) == _rows(run_local(Q1, spec))


def test_a_worker_whose_codecs_disagree_refuses_the_plan(spec: SessionSpec) -> None:
    """A codec-id mismatch is caught before any plan is decoded."""
    envelope = {
        "spec": {**spec.to_json(), "codec_ids": ["dfx_storage.physical.v1"]},
        "plan": "unused",
        "stage_id": _internal.stage_id(),
        "partition": 0,
    }
    with pytest.raises(RuntimeError, match="do not match driver's"):
        run_task(envelope)


def test_a_bare_session_carries_none_of_the_three_libraries() -> None:
    """Nothing about a `SessionContext` is installed by default.

    Every codec in :func:`expected_codec_ids` is there because
    :func:`build_session` put it there, which is why that function is the only
    supported way to build a driver or a worker.
    """
    ctx = SessionContext()
    installed = sorted(ctx.physical_extension_codec_ids())
    assert installed == []
    assert expected_codec_ids() != installed


def test_build_session_rejects_a_session_it_built_wrong(
    spec: SessionSpec, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`build_session` checks its own work before handing the session over.

    The check is what turns "a worker was built slightly differently" from a
    decode failure deep in a query into an error naming the codec ids.

    Induced by patching the expectation, because no argument can produce the
    mismatch from the other side: *which* libraries get installed is written
    into :func:`build_session`, not taken from the spec. That is what the
    check guards -- this module being edited inconsistently, a library added
    to one list and not the other -- rather than anything a caller passes.
    Its counterpart for a genuinely mismatched peer is
    `test_a_worker_whose_codecs_disagree_refuses_the_plan`, which compares a
    worker's session against the driver's envelope.
    """
    with_a_fourth = sorted([*expected_codec_ids(), "dfx_absent.physical.v1"])
    monkeypatch.setattr("dfx_engine.session.expected_codec_ids", lambda: with_a_fourth)

    with pytest.raises(RuntimeError, match="do not match the expected") as excinfo:
        build_session(spec)

    # The message names both sides and says what breaks, so the reader does
    # not have to guess which list is wrong.
    assert "dfx_absent.physical.v1" in str(excinfo.value)
    assert "will fail to decode" in str(excinfo.value)


def test_a_plan_encoded_without_a_context_cannot_be_encoded(
    spec: SessionSpec,
) -> None:
    """`to_bytes()` with no context uses an empty chain and fails.

    The driver has to pass its session. This is easy to get wrong because the
    argument is optional and the failure only appears once a library node is
    in the plan.
    """
    ctx, _engine, _storage = build_session(spec)
    stage = find_stage(ctx.sql(Q1).execution_plan())
    assert stage is not None

    with pytest.raises(Exception, match=r"(?i)codec"):
        stage.to_bytes()


def test_the_spec_round_trips_through_json(spec: SessionSpec) -> None:
    """Workers receive the spec as JSON, so it has to survive the trip."""
    restored = SessionSpec.from_json(spec.to_json())

    assert dataclasses.asdict(restored) == dataclasses.asdict(spec)
    assert spec.to_json()["codec_ids"] == expected_codec_ids()


def test_the_bundles_are_reusable_across_sessions(spec: SessionSpec) -> None:
    """Two sessions from one factory call each get their own components."""
    first, _, _ = build_session(spec)
    second, _, _ = build_session(spec)

    assert first.__datafusion_codec_id__ != second.__datafusion_codec_id__
    assert sorted(first.physical_extension_codec_ids()) == expected_codec_ids()
    assert sorted(second.physical_extension_codec_ids()) == expected_codec_ids()


def test_an_out_of_range_partition_is_reported_by_the_worker(
    spec: SessionSpec, tmp_path: pathlib.Path
) -> None:
    """The worker bounds-checks rather than letting a scan index off the end."""
    ctx, _engine, _storage = build_session(spec)
    stage = find_stage(ctx.sql(Q1).execution_plan())
    assert stage is not None
    plan_path = tmp_path / "stage.plan"
    plan_path.write_bytes(stage.to_bytes(ctx))

    envelope = {
        "spec": spec.to_json(),
        "plan": str(plan_path),
        "stage_id": _internal.stage_id(),
        "partition": 99,
    }
    with pytest.raises(RuntimeError, match=re.escape("partition 99 is out of range")):
        run_task(envelope)
