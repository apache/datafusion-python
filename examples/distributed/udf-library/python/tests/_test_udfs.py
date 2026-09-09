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

"""What a function library owes a caller who is going to ship its plans."""

from __future__ import annotations

import re
import subprocess
import sys
import textwrap
from typing import TYPE_CHECKING

import pytest
from datafusion import SessionConfig, SessionContext, udaf, udf, udwf
from datafusion.plan import ExecutionPlan
from dfx_udfs import (
    CodecObservations,
    NetRevenueUDF,
    RevenueRankUDWF,
    WeightedAvgUDAF,
)

if TYPE_CHECKING:
    import pathlib


def _session(directory: pathlib.Path, *, with_codecs: bool = True) -> tuple:
    """Build a session the way this library requires: by hand.

    There is no `with_extensions(...)` here, and that is the point. Compare
    with `dfx_storage`, which is one call. This library needs two codec
    installs and three registrations, in an order the caller has to get right
    on their own.
    """
    observations = CodecObservations()
    ctx = SessionContext(SessionConfig().with_target_partitions(2))
    if with_codecs:
        ctx = ctx.with_logical_extension_codec(observations.logical_codec())
        ctx = ctx.with_physical_extension_codec(observations.physical_codec())
    ctx.register_udf(udf(NetRevenueUDF()))
    ctx.register_udaf(udaf(WeightedAvgUDAF()))
    ctx.register_udwf(udwf(RevenueRankUDWF()))
    ctx.register_parquet("lineitem", str(directory))
    return ctx, observations


def test_the_scalar_function_computes_tpch_revenue(lineitem: pathlib.Path) -> None:
    """`price * (1 - discount) * (1 + tax)`, checked by hand."""
    ctx, _ = _session(lineitem)
    rows = ctx.sql(
        "select dfx_net_revenue(l_extendedprice, l_discount, l_tax) as revenue "
        "from lineitem order by revenue"
    ).collect()

    revenue = [value for batch in rows for value in batch.column(0).to_pylist()]
    # 100*1*1, 200*0.5*1, 400*0.75*1.1
    assert revenue == pytest.approx([100.0, 100.0, 330.0])


def test_the_aggregate_is_correct_when_split_across_partitions(
    lineitem: pathlib.Path,
) -> None:
    """The two-sum state is what makes a partial/final split come out right.

    The input is two files and the session has two target partitions, so
    DataFusion runs a partial aggregate per partition and merges. An aggregate
    that could not be computed that way would give a different answer here
    than over a single partition -- which is exactly what happens on a worker.
    """
    ctx, _ = _session(lineitem)
    plan = ctx.sql(
        "select dfx_weighted_avg(l_extendedprice, l_quantity) from lineitem"
    ).execution_plan()
    assert "AggregateExec: mode=Partial" in plan.display_indent()

    result = ctx.sql(
        "select dfx_weighted_avg(l_extendedprice, l_quantity) as wavg from lineitem"
    ).collect()[0]
    # (100*1 + 200*3 + 400*4) / (1 + 3 + 4) = 2300/8
    assert result.column(0)[0].as_py() == pytest.approx(287.5)


def test_the_window_function_runs(lineitem: pathlib.Path) -> None:
    """A window function under a name this library owns."""
    ctx, _ = _session(lineitem)
    rows = ctx.sql(
        "select dfx_revenue_rank() over (order by l_extendedprice desc) as rnk "
        "from lineitem order by rnk"
    ).collect()

    ranks = [value for batch in rows for value in batch.column(0).to_pylist()]
    assert ranks == [1, 2, 3]


def test_a_registered_session_never_reaches_the_codec(lineitem: pathlib.Path) -> None:
    """The registry is tried first, so the codec is the fallback, not the path.

    This is the fact that makes a missing worker-side setup so easy to miss:
    on the driver, where the functions are registered, the codec is never
    consulted and so a codec that was broken or absent would look fine.
    """
    ctx, observations = _session(lineitem)
    plan = ctx.sql(
        "select dfx_net_revenue(l_extendedprice, l_discount, l_tax) from lineitem"
    ).execution_plan()

    ExecutionPlan.from_bytes(ctx, plan.to_bytes(ctx))

    assert observations.decode_calls() == 0


def test_the_payload_carries_no_bytes(lineitem: pathlib.Path) -> None:
    """Encoded by name: no payload, so nothing to tag with a codec id.

    That is why `try_decode_udf` has to check the name before the buffer --
    with no id to route on, the chain offers the payload to every installed
    codec in turn.
    """
    ctx, _ = _session(lineitem)
    blob = (
        ctx.sql(
            "select dfx_net_revenue(l_extendedprice, l_discount, l_tax) from lineitem"
        )
        .execution_plan()
        .to_bytes(ctx)
    )

    assert b"dfx_net_revenue" in blob
    # No chained envelope for this function: an empty encoding is not framed.
    assert b"dfx_udfs.physical.v1" not in blob


WORKER = textwrap.dedent(
    """
    import sys
    from datafusion import SessionContext
    from datafusion.plan import ExecutionPlan
    from datafusion import udaf, udf, udwf
    from dfx_udfs import (
        CodecObservations, NetRevenueUDF, RevenueRankUDWF, WeightedAvgUDAF,
    )

    blob_path, mode = sys.argv[1], sys.argv[2]
    observations = CodecObservations()
    ctx = SessionContext()

    if mode == "codec":
        # Install the codec and register nothing. The functions are rebuilt
        # from their names.
        ctx = ctx.with_physical_extension_codec(observations.physical_codec())
    elif mode == "registry":
        # The mirror image: register the functions, install no codec.
        ctx.register_udf(udf(NetRevenueUDF()))
        ctx.register_udaf(udaf(WeightedAvgUDAF()))
        ctx.register_udwf(udwf(RevenueRankUDWF()))
    elif mode == "neither":
        pass

    ctx.register_parquet("lineitem", sys.argv[3])
    with open(blob_path, "rb") as handle:
        plan = ExecutionPlan.from_bytes(ctx, handle.read())

    total = 0.0
    for partition in range(plan.partition_count):
        for batch in ctx.execute(plan, partition):
            total += sum(batch.to_pyarrow().column(0).to_pylist())
    print(f"total={total:.1f} decoded={observations.decode_calls()}")
    """
)


def _run_worker(
    tmp_path: pathlib.Path, blob: bytes, mode: str, data: pathlib.Path
) -> subprocess.CompletedProcess[str]:
    blob_path = tmp_path / "plan.bin"
    blob_path.write_bytes(blob)
    worker = tmp_path / "worker.py"
    worker.write_text(WORKER)
    return subprocess.run(  # noqa: S603
        [sys.executable, str(worker), str(blob_path), mode, str(data)],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def revenue_plan(lineitem: pathlib.Path) -> bytes:
    ctx, _ = _session(lineitem)
    return (
        ctx.sql(
            "select dfx_net_revenue(l_extendedprice, l_discount, l_tax) from lineitem"
        )
        .execution_plan()
        .to_bytes(ctx)
    )


def test_a_worker_with_only_the_codec_can_run_the_plan(
    revenue_plan: bytes, lineitem: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """Installing the codec is an alternative to registering the functions.

    A separate process that has never registered `dfx_net_revenue` rebuilds it
    from the name in the plan.
    """
    result = _run_worker(tmp_path, revenue_plan, "codec", lineitem)

    assert result.returncode == 0, result.stderr
    assert "total=530.0" in result.stdout
    # The codec, not a registry hit, is what answered.
    assert "decoded=1" in result.stdout


def test_a_worker_with_only_the_registrations_can_run_the_plan(
    revenue_plan: bytes, lineitem: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """And registering the functions is an alternative to the codec."""
    result = _run_worker(tmp_path, revenue_plan, "registry", lineitem)

    assert result.returncode == 0, result.stderr
    assert "total=530.0" in result.stdout
    assert "decoded=0" in result.stdout


def test_a_worker_with_neither_names_the_function_it_cannot_find(
    revenue_plan: bytes, lineitem: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """The failure is legible, and it arrives at decode rather than at execute.

    This is the whole cost of a function library that ships without a codec
    and without documenting what a worker must register.
    """
    result = _run_worker(tmp_path, revenue_plan, "neither", lineitem)

    assert result.returncode != 0
    assert "dfx_net_revenue" in result.stderr


def test_the_codec_declines_names_it_does_not_own(lineitem: pathlib.Path) -> None:
    """A name-only payload reaches every codec, so declining matters.

    With no bytes there is no codec id to route on. A codec that answered for
    any name it was handed would hijack another library's functions.
    """
    ctx, observations = _session(lineitem, with_codecs=True)
    # `abs` is a built-in, so the plan references a name this library does not
    # own; decoding offers it around.
    blob = (
        ctx.sql("select abs(l_discount) from lineitem").execution_plan().to_bytes(ctx)
    )
    ExecutionPlan.from_bytes(ctx, blob)

    assert observations.decode_calls() == 0


def test_the_codec_ids_are_pinned() -> None:
    """Renaming the exporting class must not invalidate written plans."""
    observations = CodecObservations()

    assert observations.logical_codec().__datafusion_codec_id__ == "dfx_udfs.logical.v1"
    assert (
        observations.physical_codec().__datafusion_codec_id__ == "dfx_udfs.physical.v1"
    )


def test_this_library_cannot_be_installed_as_a_bundle() -> None:
    """The mixed-workflow case, asserted rather than described.

    `SessionExtensionComponents` carries codec fields only, so a function
    library has nowhere to put its functions and this one does not pretend
    otherwise. `with_extensions` rejects it by name.
    """
    observations = CodecObservations()

    assert not hasattr(observations, "__datafusion_session_components__")
    assert not hasattr(observations, "__datafusion_session_planner__")

    with pytest.raises(TypeError, match=re.escape("__datafusion_session_components__")):
        SessionContext().with_extensions(observations)
