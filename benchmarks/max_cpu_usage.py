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
"""Benchmark script showing how to maximize CPU usage."""

from __future__ import annotations

import argparse
import multiprocessing
import time

import pyarrow as pa
from datafusion import SessionConfig, SessionContext, col
from datafusion import functions as f


def main(num_rows: int, partitions: int) -> None:
    """Run a simple aggregation after repartitioning."""
    # Create some example data
    array = pa.array(range(num_rows))
    batch = pa.record_batch([array], names=["a"])

    # Configure the session to use a higher target partition count and
    # enable automatic repartitioning.
    config = (
        SessionConfig()
        .with_target_partitions(partitions)
        .with_repartition_joins(enabled=True)
        .with_repartition_aggregations(enabled=True)
        .with_repartition_windows(enabled=True)
    )
    ctx = SessionContext(config)

    # Register the input data and repartition manually to ensure that all
    # partitions are used.
    df = ctx.create_dataframe([[batch]]).repartition(partitions)

    start = time.time()
    df = df.aggregate([], [f.sum(col("a"))])
    df.collect()
    end = time.time()

    print(
        f"Processed {num_rows} rows using {partitions} partitions in {end - start:.3f}s"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rows",
        type=int,
        default=1_000_000,
        help="Number of rows in the generated dataset",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=multiprocessing.cpu_count(),
        help="Target number of partitions to use",
    )
    args = parser.parse_args()
    main(args.rows, args.partitions)
