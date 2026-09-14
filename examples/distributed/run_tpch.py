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

    uv pip install tpchgen-cli
    python examples/distributed/run_tpch.py --partitions 4

Generates its own `lineitem` with `tpchgen-cli`, which shards natively: one
`tpchgen-cli parquet --parts N` call writes N Parquet files. That is also a
fair illustration of the real constraint -- a distributed engine can only
spread work as widely as the data is split -- so the number of files is the
same `--partitions` the engine is told to use.
"""

from __future__ import annotations

import argparse
import pathlib
import shutil
import subprocess
import tempfile
import time

import pyarrow as pa
from dfx_engine.driver import run_distributed, run_local
from dfx_engine.session import SessionSpec

# Q1 without the `l_shipdate` filter and the `avg` columns. The shape that
# matters is unchanged: group by two low-cardinality columns, aggregate, order.
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


def generate(into: pathlib.Path, partitions: int, scale: float) -> pathlib.Path:
    """Write `lineitem` as `partitions` Parquet files, and return their directory.

    `tpchgen-cli` puts a sharded table in a subdirectory named for it, so the
    directory this returns is `into/lineitem` -- which is what the storage
    library's table provider wants, since it scans `*.parquet` under a
    directory and makes one partition per file.
    """
    executable = shutil.which("tpchgen-cli")
    if executable is None:
        message = (
            "tpchgen-cli not found on PATH; install it with "
            "`uv pip install tpchgen-cli`"
        )
        raise RuntimeError(message)

    subprocess.run(  # noqa: S603
        [
            executable,
            "parquet",
            f"--scale-factor={scale}",
            "--tables=lineitem",
            f"--parts={partitions}",
            f"--output-dir={into}",
            "--no-progress",
            "--quiet",
        ],
        check=True,
    )
    return into / "lineitem"


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
            if lhs is None or rhs is None:
                # Checked before the float branch, which would raise
                # `TypeError` on `None - None` and report a null-handling
                # difference between the two runs as a crash in the checker.
                close = lhs is None and rhs is None
            elif isinstance(lhs, float):
                close = abs(lhs - rhs) <= 1e-6 * max(1.0, abs(rhs))
            else:
                close = lhs == rhs
            if not close:
                message = f"{name}: {lhs!r} distributed, {rhs!r} local"
                raise ValueError(message)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument(
        "--scale",
        type=float,
        default=0.1,
        help="TPC-H scale factor; 1 is the full ~6M row lineitem",
    )
    args = parser.parse_args(argv)

    workspace = pathlib.Path(tempfile.mkdtemp(prefix="dfx-tpch-"))
    try:
        data = generate(workspace, args.partitions, args.scale)
        count = len(list(data.glob("*.parquet")))
        print(f"generated {count} file(s) under {data}")

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
