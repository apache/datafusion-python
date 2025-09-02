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
from pathlib import Path

import pyarrow as pa
import pytest
from datafusion import column
from datafusion._testing import range_table
from datafusion.io import read_avro, read_csv, read_json, read_parquet


def test_read_json_global_ctx(ctx):
    path = Path(__file__).parent.resolve()

    # Default
    test_data_path = Path(path) / "data_test_context" / "data.json"
    df = read_json(test_data_path)
    result = df.collect()

    assert result[0].column(0) == pa.array(["a", "b", "c"])
    assert result[0].column(1) == pa.array([1, 2, 3])

    # Schema
    schema = pa.schema(
        [
            pa.field("A", pa.string(), nullable=True),
        ]
    )
    df = read_json(test_data_path, schema=schema)
    result = df.collect()

    assert result[0].column(0) == pa.array(["a", "b", "c"])
    assert result[0].schema == schema

    # File extension
    test_data_path = Path(path) / "data_test_context" / "data.json"
    df = read_json(test_data_path, file_extension=".json")
    result = df.collect()

    assert result[0].column(0) == pa.array(["a", "b", "c"])
    assert result[0].column(1) == pa.array([1, 2, 3])


def test_read_parquet_global():
    parquet_df = read_parquet(path="parquet/data/alltypes_plain.parquet")
    parquet_df.show()
    assert parquet_df is not None

    path = Path.cwd() / "parquet/data/alltypes_plain.parquet"
    parquet_df = read_parquet(path=path)
    assert parquet_df is not None


def test_read_csv():
    csv_df = read_csv(path="testing/data/csv/aggregate_test_100.csv")
    csv_df.select(column("c1")).show()


def test_read_csv_list():
    csv_df = read_csv(path=["testing/data/csv/aggregate_test_100.csv"])
    expected = csv_df.count() * 2

    double_csv_df = read_csv(
        path=[
            "testing/data/csv/aggregate_test_100.csv",
            "testing/data/csv/aggregate_test_100.csv",
        ]
    )
    actual = double_csv_df.count()

    double_csv_df.select(column("c1")).show()
    assert actual == expected


def test_read_avro():
    avro_df = read_avro(path="testing/data/avro/alltypes_plain.avro")
    avro_df.show()
    assert avro_df is not None

    path = Path.cwd() / "testing/data/avro/alltypes_plain.avro"
    avro_df = read_avro(path=path)
    assert avro_df is not None


def test_arrow_c_stream_large_dataset(ctx):
    """DataFrame.__arrow_c_stream__ yields batches incrementally.

    This test constructs a DataFrame that would be far larger than available
    memory if materialized. The ``__arrow_c_stream__`` method should expose a
    stream of record batches without collecting the full dataset, so reading a
    handful of batches should not exhaust process memory.
    """
    # Create a very large DataFrame using range; this would be terabytes if collected
    df = range_table(ctx, 0, 1 << 40)

    reader = pa.RecordBatchReader._import_from_c_capsule(df.__arrow_c_stream__())

    # Track RSS before consuming batches
    psutil = pytest.importorskip("psutil")
    process = psutil.Process()
    start_rss = process.memory_info().rss

    for _ in range(5):
        batch = reader.read_next_batch()
        assert batch is not None
        assert len(batch) > 0
        current_rss = process.memory_info().rss
        # Ensure memory usage hasn't grown substantially (>50MB)
        assert current_rss - start_rss < 50 * 1024 * 1024


def test_table_from_batches_stream(ctx, fail_collect):
    df = range_table(ctx, 0, 10)

    table = pa.Table.from_batches(df)
    assert table.shape == (10, 1)
    assert table.column_names == ["value"]
