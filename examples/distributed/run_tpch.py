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

"""Run TPC-H Q1 across worker processes, and compare against one process.

    python examples/distributed/run_tpch.py --partitions 4

Needs the TPC-H data the repository's other examples use::

    mkdir -p examples/tpch/data && cd examples/tpch/data
    uv pip install tpchgen-cli && uv run --no-project tpchgen-cli -s 1 --format=parquet

`tpchgen-cli` writes one file per table, so `lineitem.parquet` is a single
220 MB file -- one partition, and nothing to fan out. This script re-shards
the columns Q1 needs into `--partitions` files first, which is also a fair
illustration of the real constraint: a distributed engine can only spread work
as widely as the data is split.
"""

from __future__ import annotations

import argparse
import pathlib
import shutil
import sys
import tempfile
import time

import pyarrow as pa
import pyarrow.parquet as pq
from dfx_engine.driver import run_distributed, run_local
from dfx_engine.session import SessionSpec

# Q1 without the `l_shipdate` filter and the `avg` columns, so the shard below
# stays small. The shape that matters is unchanged: group by two low-cardinality
# columns, aggregate, order.
Q1 = """
select l_returnflag,
       l_linestatus,
       count(*)                 as count_order,
       sum(l_quantity)          as sum_qty,
       sum(l_extendedprice)     as sum_base_price,
       sum(dfx_net_revenue(l_extendedprice, l_discount, l_tax)) as sum_charge,
       dfx_weighted_avg(l_extendedprice, l_quantity)            as wavg_price
from lineitem
group by l_returnflag, l_linestatus
order by l_returnflag, l_linestatus
"""

COLUMNS = [
    "l_returnflag",
    "l_linestatus",
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_tax",
]


def reshard(
    source: pathlib.Path, into: pathlib.Path, partitions: int, rows: int
) -> int:
    """Write the first `rows` rows of `source` as `partitions` Parquet files."""
    into.mkdir(parents=True, exist_ok=True)
    table = pq.read_table(source, columns=COLUMNS)
    if rows:
        table = table.slice(0, rows)

    per_file = max(1, table.num_rows // partitions)
    written = 0
    for index in range(partitions):
        offset = index * per_file
        length = table.num_rows - offset if index == partitions - 1 else per_file
        if length <= 0:
            break
        pq.write_table(table.slice(offset, length), into / f"part-{index}.parquet")
        written += 1
    return written


def compare(table: pa.Table, reference: pa.Table) -> None:
    """Raise unless `table` matches `reference`, floats to 1e-6 relative.

    Floats get a tolerance rather than equality. Splitting a `sum` across
    partitions changes the order the additions happen in, and floating point
    addition is not associative, so the last bits of `sum_charge` legitimately
    differ between the two runs. Every distributed engine has this property;
    it is worth knowing before someone diffs two runs and concludes the split
    is broken.
    """
    if table.column_names != reference.column_names:
        message = (
            f"column names differ: {table.column_names} vs {reference.column_names}"
        )
        raise ValueError(message)

    for name in table.column_names:
        got = table.column(name).to_pylist()
        want = reference.column(name).to_pylist()
        if len(got) != len(want):
            message = f"{name}: {len(got)} rows distributed, {len(want)} local"
            raise ValueError(message)
        for lhs, rhs in zip(got, want, strict=True):
            close = (
                abs(lhs - rhs) <= 1e-6 * max(1.0, abs(rhs))
                if isinstance(lhs, float)
                else lhs == rhs
            )
            if not close:
                message = f"{name}: {lhs!r} distributed, {rhs!r} local"
                raise ValueError(message)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data",
        type=pathlib.Path,
        default=pathlib.Path(__file__).resolve().parents[1]
        / "tpch"
        / "data"
        / "lineitem.parquet",
    )
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument(
        "--rows",
        type=int,
        default=2_000_000,
        help="rows to use; 0 for all of them (SF 1 lineitem is ~6M)",
    )
    args = parser.parse_args(argv)

    if not args.data.exists():
        sys.stderr.write(
            f"{args.data} not found. Generate it with:\n"
            "  mkdir -p examples/tpch/data && cd examples/tpch/data\n"
            "  uv pip install tpchgen-cli\n"
            "  uv run --no-project tpchgen-cli -s 1 --format=parquet\n"
        )
        return 2

    workspace = pathlib.Path(tempfile.mkdtemp(prefix="dfx-tpch-"))
    try:
        data = workspace / "lineitem"
        count = reshard(args.data, data, args.partitions, args.rows)
        print(f"resharded into {count} file(s) under {data}")

        spec = SessionSpec(
            tables={"lineitem": str(data)},
            shuffle_dir=str(workspace / "shuffle"),
            target_partitions=args.partitions,
        )

        start = time.monotonic()
        result = run_distributed(Q1, spec)
        distributed = time.monotonic() - start
        print(
            f"distributed: {distributed:.2f}s across {len(result.tasks)} "
            f"worker process(es); rows per (stage, partition) {result.task_rows}"
        )

        start = time.monotonic()
        local = run_local(Q1, spec)
        print(f"single process: {time.monotonic() - start:.2f}s")

        # The point of the comparison is agreement, not speed: four processes
        # on one laptop will not beat one process that skips the round trip
        # through Arrow IPC files.
        table = pa.Table.from_batches(result.batches)
        reference = pa.Table.from_batches(local)

        # Raises rather than asserts. This comparison is the only thing that
        # makes the script a check rather than a demo, and `python -O` removes
        # an `assert` -- which would leave it printing a table it never
        # verified.
        compare(table, reference)

        print("\nsame answer both ways (floats to within 1e-6 relative):\n")
        names = table.column_names
        print("  ".join(f"{name:>16}" for name in names))
        for row in zip(
            *(table.column(name).to_pylist() for name in names), strict=True
        ):
            print("  ".join(f"{value:>16}" for value in row))
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
