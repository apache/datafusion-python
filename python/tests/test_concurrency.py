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

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
from datafusion import Config, SessionContext, col, lit
from datafusion import functions as f
from datafusion.common import SqlSchema


def _run_in_threads(fn, count: int = 8) -> None:
    with ThreadPoolExecutor(max_workers=count) as executor:
        futures = [executor.submit(fn, i) for i in range(count)]
        for future in futures:
            # Propagate any exception raised in the worker thread.
            future.result()


def test_concurrent_access_to_shared_structures() -> None:
    """Exercise SqlSchema, Config, and DataFrame concurrently."""

    schema = SqlSchema("concurrency")
    config = Config()
    ctx = SessionContext()

    batch = pa.record_batch([pa.array([1, 2, 3], type=pa.int32())], names=["value"])
    df = ctx.create_dataframe([[batch]])

    config_key = "datafusion.execution.batch_size"
    expected_rows = batch.num_rows

    def worker(index: int) -> None:
        schema.name = f"concurrency-{index}"
        assert schema.name.startswith("concurrency-")
        # Exercise getters that use internal locks.
        assert isinstance(schema.tables, list)
        assert isinstance(schema.views, list)
        assert isinstance(schema.functions, list)

        config.set(config_key, str(1024 + index))
        assert config.get(config_key) is not None
        # Access the full config map to stress lock usage.
        assert config_key in config.get_all()

        batches = df.collect()
        assert sum(batch.num_rows for batch in batches) == expected_rows

    _run_in_threads(worker, count=12)


def test_case_builder_reuse_from_multiple_threads() -> None:
    """Ensure the case builder can be safely reused across threads."""

    ctx = SessionContext()
    values = pa.array([0, 1, 2, 3, 4], type=pa.int32())
    df = ctx.create_dataframe([[pa.record_batch([values], names=["value"])]])

    base_builder = f.case(col("value"))

    def add_case(i: int) -> None:
        base_builder.when(lit(i), lit(f"value-{i}"))

    _run_in_threads(add_case, count=8)

    with ThreadPoolExecutor(max_workers=2) as executor:
        otherwise_future = executor.submit(base_builder.otherwise, lit("default"))
        case_expr = otherwise_future.result()

    result = df.select(case_expr.alias("label")).collect()
    assert sum(batch.num_rows for batch in result) == len(values)

    predicate_builder = f.when(col("value") == lit(0), lit("zero"))

    def add_predicate(i: int) -> None:
        predicate_builder.when(col("value") == lit(i + 1), lit(f"value-{i + 1}"))

    _run_in_threads(add_predicate, count=4)

    with ThreadPoolExecutor(max_workers=2) as executor:
        end_future = executor.submit(predicate_builder.end)
        predicate_expr = end_future.result()

    result = df.select(predicate_expr.alias("label")).collect()
    assert sum(batch.num_rows for batch in result) == len(values)
