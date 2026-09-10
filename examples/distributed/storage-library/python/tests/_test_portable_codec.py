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

"""The claim this library exists to make: its plans decode in another process.

Every other example codec in this repository parks the live object in a
process-global map and encodes a token. These tests are written to fail if
this one ever does that -- the decoding side is a separate interpreter, so a
token would have nothing to look up.
"""

from __future__ import annotations

import itertools
import re
import subprocess
import sys
import textwrap
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from datafusion import SessionContext
from datafusion.plan import ExecutionPlan, LogicalPlan
from dfx_storage import DfxStorageExtension, PartitionedParquetTable

if TYPE_CHECKING:
    import pathlib


def _configured(directory: pathlib.Path) -> tuple[SessionContext, DfxStorageExtension]:
    bundle = DfxStorageExtension()
    ctx = SessionContext().with_extensions(bundle)
    ctx.register_table("readings", PartitionedParquetTable(str(directory)))
    return ctx, bundle


def test_provider_reports_one_partition_per_file(readings_dir: pathlib.Path) -> None:
    """The file is the partition, which is the axis an engine splits along."""
    ctx, _ = _configured(readings_dir)
    plan = ctx.sql("select sensor_id from readings").execution_plan()

    assert plan.partition_count == 3
    # Not `Hash`: rows are grouped by which file they landed in, which says
    # nothing about their values.
    assert plan.output_partitioning.scheme == "UnknownPartitioning"
    assert plan.output_partitioning.hash_expressions is None


def test_each_partition_reads_exactly_one_file(readings_dir: pathlib.Path) -> None:
    """Partition i reads file i, so two workers never read the same bytes."""
    ctx, _ = _configured(readings_dir)
    plan = ctx.sql("select sensor_id from readings").execution_plan()

    per_partition = [
        sorted(
            value
            for batch in ctx.execute(plan, partition)
            for value in batch.to_pyarrow().column("sensor_id").to_pylist()
        )
        for partition in range(plan.partition_count)
    ]

    assert per_partition == [[0, 1, 2], [100, 101, 102], [200, 201, 202]]
    # Disjoint, and together the whole table.
    everything = sorted(itertools.chain.from_iterable(per_partition))
    assert everything == [0, 1, 2, 100, 101, 102, 200, 201, 202]


def test_this_librarys_codec_carried_the_node(readings_dir: pathlib.Path) -> None:
    """Assert *this* codec did the work, not merely that the query succeeded.

    Both codecs being installed does not mean this one saw the node; a codec
    installed earlier that claims broadly would have taken it.
    """
    ctx, bundle = _configured(readings_dir)
    plan = ctx.sql("select sensor_id from readings").execution_plan()
    assert bundle.encode_calls() == 0

    blob = plan.to_bytes(ctx)
    assert bundle.encode_calls() == 1

    restored = ExecutionPlan.from_bytes(ctx, blob)
    assert bundle.decode_calls() == 1
    assert "PartitionedParquetExec" in restored.display_indent()


def test_the_payload_is_metadata_not_a_token(readings_dir: pathlib.Path) -> None:
    """The bytes name the files, so they mean something in another process."""
    ctx, _ = _configured(readings_dir)
    blob = ctx.sql("select sensor_id from readings").execution_plan().to_bytes(ctx)

    assert b"DFXSTOR1" in blob
    for index in range(3):
        assert f"part-{index}.parquet".encode() in blob


def test_the_same_bytes_decode_twice(readings_dir: pathlib.Path) -> None:
    """A token registry consumes its entry on decode. Durable metadata does not.

    This is what lets one encoded plan fan out to several workers.
    """
    ctx, bundle = _configured(readings_dir)
    blob = ctx.sql("select sensor_id from readings").execution_plan().to_bytes(ctx)

    first = ExecutionPlan.from_bytes(ctx, blob)
    second = ExecutionPlan.from_bytes(ctx, blob)

    assert bundle.decode_calls() == 2
    assert first.partition_count == second.partition_count == 3


def test_a_projection_survives_the_round_trip(readings_dir: pathlib.Path) -> None:
    """The projection is part of the descriptor, not re-derived on decode."""
    ctx, _ = _configured(readings_dir)
    plan = ctx.sql("select reading from readings").execution_plan()
    restored = ExecutionPlan.from_bytes(ctx, plan.to_bytes(ctx))

    rows = [
        value
        for partition in range(restored.partition_count)
        for batch in ctx.execute(restored, partition)
        for value in batch.to_pyarrow().column("reading").to_pylist()
    ]
    assert sorted(rows) == [1.5, 1.5, 1.5, 2.5, 2.5, 2.5, 3.5, 3.5, 3.5]


def test_stock_nodes_never_reach_this_codec(readings_dir: pathlib.Path) -> None:
    """An extension codec is only consulted for nodes with no native encoding.

    The aggregate and filter above the scan all have their own `try_to_proto`,
    so the framework encodes them itself and this codec is never offered them.
    That is why claiming a broad category is so damaging: the only nodes that
    ever arrive here are ones *some* library owns, so a broad claim can only
    ever steal from a peer, never pick up slack.
    """
    ctx, bundle = _configured(readings_dir)
    plan = ctx.sql("select count(*) from readings where reading > 2.0").execution_plan()

    plan.to_bytes(ctx)

    # Exactly one node in that plan is ours, and nothing else was offered.
    assert bundle.encode_calls() == 1
    assert bundle.declined_calls() == 0


def test_a_malformed_projection_is_refused_not_dropped(
    readings_dir: pathlib.Path,
) -> None:
    """A projection index that will not parse has to be an error.

    Dropping it instead would hand back a shorter projection, and because the
    indices are positional that is a different query rather than a degraded
    one -- losing one reads the wrong columns and losing all of them reads
    none, with a well-formed plan either way. A codec is the last place that
    can tell a malformed payload from a valid one.

    The payload is edited in place, keeping its length: the JSON sits inside a
    length-delimited protobuf field, so `[1,2]` is replaced by the same-width
    `[1e1]`, which is a valid JSON *float* and so not a column number. Writing
    a shorter or longer payload would corrupt the protobuf instead and test
    the wrong thing.
    """
    ctx, _ = _configured(readings_dir)
    plan = ctx.sql("select sensor_id, reading from readings").execution_plan()
    blob = plan.to_bytes(ctx)
    assert b'"projection":[0,1]' in blob

    mangled = blob.replace(b'"projection":[0,1]', b'"projection":[1e1]')
    assert len(mangled) == len(blob), "the edit has to preserve the protobuf framing"

    with pytest.raises(Exception, match="is not a column number") as excinfo:
        ExecutionPlan.from_bytes(ctx, mangled)

    # A bad payload is `Execution`, not `Internal`. DataFusion appends a
    # "please file a bug report" line to every internal error, and sending
    # someone to DataFusion's issue tracker over a corrupt payload of ours
    # wastes their time and the maintainers'.
    assert "bug report" not in str(excinfo.value)
    assert "Internal error" not in str(excinfo.value)


def test_the_logical_codec_carries_the_provider(readings_dir: pathlib.Path) -> None:
    """The provider is held in the logical plan, so it needs its own codec.

    A physical codec is not enough. Nothing here installs a query planner, so
    this is the only test that reaches the logical half directly -- but any
    session with an engine installed takes this path on every query, which is
    what makes a provider library shipping only a physical codec fail as soon
    as it meets one.
    """
    ctx, bundle = _configured(readings_dir)
    plan = ctx.sql("select sensor_id from readings").logical_plan()
    assert bundle.provider_encode_calls() == 0

    blob = plan.to_bytes(ctx)
    assert bundle.provider_encode_calls() == 1
    # The directory, under the logical payload's own magic.
    assert b"DFXSTOL1" in blob
    assert str(readings_dir).encode() in blob

    LogicalPlan.from_bytes(ctx, blob)
    assert bundle.provider_decode_calls() == 1


def test_a_decoded_provider_reports_the_schema_from_the_plan(
    readings_dir: pathlib.Path,
) -> None:
    """The decoder takes the plan's schema rather than re-reading a footer.

    A serialized scan carries its projection as column *names*, which the
    decoder resolves to indices against the schema in the plan and then
    applies to whatever this provider reports -- without a bounds check. So
    the two have to be the same schema.

    Here the directory gains a column in front of the others after the plan
    was written. Re-reading the footer on decode would make index 0 mean
    `label` while the plan means `sensor_id`, and the query would quietly
    return the wrong column.
    """
    ctx, _ = _configured(readings_dir)
    blob = ctx.sql("select sensor_id from readings").logical_plan().to_bytes(ctx)

    pq.write_table(
        pa.table(
            {
                "label": ["a", "b", "c"],
                "sensor_id": [0, 1, 2],
                "reading": [1.5, 2.5, 3.5],
            }
        ),
        readings_dir / "part-0.parquet",
    )

    restored = LogicalPlan.from_bytes(ctx, blob)

    # Still the schema the plan was built against: `label` is not in it, and
    # the projected column is the one that was asked for.
    schema_text = restored.display_indent_schema()
    assert "sensor_id" in schema_text
    assert "label" not in schema_text


WORKER = textwrap.dedent(
    """
    import sys
    from datafusion import SessionContext
    from datafusion.plan import ExecutionPlan
    from dfx_storage import DfxStorageExtension

    blob_path, expected_id = sys.argv[1], sys.argv[2]

    # A session built from scratch: this process has never registered the
    # table, and shares nothing with the one that wrote the plan.
    bundle = DfxStorageExtension()
    ctx = SessionContext().with_extensions(bundle)

    installed = ctx.physical_extension_codec_ids()
    assert expected_id in installed, f"codec id {expected_id} not in {installed}"

    with open(blob_path, "rb") as handle:
        plan = ExecutionPlan.from_bytes(ctx, handle.read())

    total = 0
    for partition in range(plan.partition_count):
        for batch in ctx.execute(plan, partition):
            total += batch.to_pyarrow().num_rows
    print(f"partitions={plan.partition_count} rows={total} decoded={bundle.decode_calls()}")
    """
)


def test_a_separate_process_decodes_and_executes_the_plan(
    readings_dir: pathlib.Path, tmp_path: pathlib.Path
) -> None:
    """The whole point. No shared session, no shared registry, no token.

    Spawned through `sys.executable` rather than `multiprocessing`: the tokio
    runtime backing this extension is a process-global, so `fork` is unsafe,
    and a hardcoded `python` could differ in minor version from this one.
    """
    ctx, _ = _configured(readings_dir)
    blob = ctx.sql("select sensor_id from readings").execution_plan().to_bytes(ctx)
    blob_path = tmp_path / "plan.bin"
    blob_path.write_bytes(blob)

    worker = tmp_path / "worker.py"
    worker.write_text(WORKER)

    result = subprocess.run(  # noqa: S603
        [sys.executable, str(worker), str(blob_path), "dfx_storage.physical.v1"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "partitions=3 rows=9 decoded=1" in result.stdout


def test_a_worker_without_the_codec_says_which_one_is_missing(
    readings_dir: pathlib.Path,
) -> None:
    """The failure names the codec, which is the whole value of pinned ids."""
    ctx, _ = _configured(readings_dir)
    blob = ctx.sql("select sensor_id from readings").execution_plan().to_bytes(ctx)

    # A session with no extension codecs at all, standing in for a worker
    # whose bootstrap forgot to install the bundle.
    bare = SessionContext()
    with pytest.raises(
        Exception, match=re.escape("dfx_storage.physical.v1")
    ) as excinfo:
        ExecutionPlan.from_bytes(bare, blob)
    assert "not installed on this session" in str(excinfo.value)


def test_the_bundle_is_reusable_across_sessions(readings_dir: pathlib.Path) -> None:
    """One bundle object, two sessions: components are built per install."""
    bundle = DfxStorageExtension()
    first = SessionContext().with_extensions(bundle)
    second = SessionContext().with_extensions(bundle)

    for ctx in (first, second):
        ctx.register_table("readings", PartitionedParquetTable(str(readings_dir)))
        assert (
            ctx.sql("select count(*) from readings").collect()[0].column(0)[0].as_py()
            == 9
        )

    assert first.__datafusion_codec_id__ != second.__datafusion_codec_id__
