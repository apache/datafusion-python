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

import os

import pyarrow as pa
import pyarrow.dataset as ds

from datafusion import column, literal, SessionContext
import pytest


def test_create_context_no_args():
    SessionContext()


def test_create_context_with_all_valid_args():
    ctx = SessionContext(
        target_partitions=1,
        default_catalog="foo",
        default_schema="bar",
        create_default_catalog_and_schema=True,
        information_schema=True,
        repartition_joins=False,
        repartition_aggregations=False,
        repartition_windows=False,
        parquet_pruning=False,
        config_options=None,
    )

    # verify that at least some of the arguments worked
    ctx.catalog("foo").database("bar")
    with pytest.raises(KeyError):
        ctx.catalog("datafusion")


def test_register_record_batches(ctx):
    # create a RecordBatch and register it as memtable
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )

    ctx.register_record_batches("t", [[batch]])

    assert ctx.tables() == {"t"}

    result = ctx.sql("SELECT a+b, a-b FROM t").collect()

    assert result[0].column(0) == pa.array([5, 7, 9])
    assert result[0].column(1) == pa.array([-3, -3, -3])


def test_create_dataframe_registers_unique_table_name(ctx):
    # create a RecordBatch and register it as memtable
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )

    df = ctx.create_dataframe([[batch]])
    tables = list(ctx.tables())

    assert df
    assert len(tables) == 1
    assert len(tables[0]) == 33
    assert tables[0].startswith("c")
    # ensure that the rest of the table name contains
    # only hexadecimal numbers
    for c in tables[0][1:]:
        assert c in "0123456789abcdef"


def test_register_table(ctx, database):
    default = ctx.catalog()
    public = default.database("public")
    assert public.names() == {"csv", "csv1", "csv2"}
    table = public.table("csv")

    ctx.register_table("csv3", table)
    assert public.names() == {"csv", "csv1", "csv2", "csv3"}


def test_deregister_table(ctx, database):
    default = ctx.catalog()
    public = default.database("public")
    assert public.names() == {"csv", "csv1", "csv2"}

    ctx.deregister_table("csv")
    assert public.names() == {"csv1", "csv2"}


def test_register_dataset(ctx):
    # create a RecordBatch and register it as a pyarrow.dataset.Dataset
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    dataset = ds.dataset([batch])
    ctx.register_dataset("t", dataset)

    assert ctx.tables() == {"t"}

    result = ctx.sql("SELECT a+b, a-b FROM t").collect()

    assert result[0].column(0) == pa.array([5, 7, 9])
    assert result[0].column(1) == pa.array([-3, -3, -3])


def test_dataset_filter(ctx, capfd):
    # create a RecordBatch and register it as a pyarrow.dataset.Dataset
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    dataset = ds.dataset([batch])
    ctx.register_dataset("t", dataset)

    assert ctx.tables() == {"t"}
    df = ctx.sql("SELECT a+b, a-b FROM t WHERE a BETWEEN 2 and 3 AND b > 5")

    # Make sure the filter was pushed down in Physical Plan
    df.explain()
    captured = capfd.readouterr()
    assert "filter_expr=(((a >= 2) and (a <= 3)) and (b > 5))" in captured.out

    result = df.collect()

    assert result[0].column(0) == pa.array([9])
    assert result[0].column(1) == pa.array([-3])


def test_dataset_filter_nested_data(ctx):
    # create Arrow StructArrays to test nested data types
    data = pa.StructArray.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    batch = pa.RecordBatch.from_arrays(
        [data],
        names=["nested_data"],
    )
    dataset = ds.dataset([batch])
    ctx.register_dataset("t", dataset)

    assert ctx.tables() == {"t"}

    df = ctx.table("t")

    # This filter will not be pushed down to DatasetExec since it isn't supported
    df = df.select(
        column("nested_data")["a"] + column("nested_data")["b"],
        column("nested_data")["a"] - column("nested_data")["b"],
    ).filter(column("nested_data")["b"] > literal(5))

    result = df.collect()

    assert result[0].column(0) == pa.array([9])
    assert result[0].column(1) == pa.array([-3])


def test_table_exist(ctx):
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    dataset = ds.dataset([batch])
    ctx.register_dataset("t", dataset)

    assert ctx.table_exist("t") is True


def test_read_json(ctx):
    path = os.path.dirname(os.path.abspath(__file__))

    # Default
    test_data_path = os.path.join(path, "data_test_context", "data.json")
    df = ctx.read_json(test_data_path)
    result = df.collect()

    assert result[0].column(0) == pa.array(["a", "b", "c"])
    assert result[0].column(1) == pa.array([1, 2, 3])

    # Schema
    schema = pa.schema(
        [
            pa.field("A", pa.string(), nullable=True),
        ]
    )
    df = ctx.read_json(test_data_path, schema=schema)
    result = df.collect()

    assert result[0].column(0) == pa.array(["a", "b", "c"])
    assert result[0].schema == schema

    # File extension
    test_data_path = os.path.join(path, "data_test_context", "data.json")
    df = ctx.read_json(test_data_path, file_extension=".json")
    result = df.collect()

    assert result[0].column(0) == pa.array(["a", "b", "c"])
    assert result[0].column(1) == pa.array([1, 2, 3])


def test_read_csv(ctx):
    csv_df = ctx.read_csv(path="testing/data/csv/aggregate_test_100.csv")
    csv_df.select(column("c1")).show()


def test_read_parquet(ctx):
    csv_df = ctx.read_parquet(path="parquet/data/alltypes_plain.parquet")
    csv_df.show()


def test_read_avro(ctx):
    csv_df = ctx.read_avro(path="testing/data/avro/alltypes_plain.avro")
    csv_df.show()
