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
import gzip
import os
import datetime as dt
import pathlib

import pyarrow as pa
import pyarrow.dataset as ds
import pytest

from datafusion import (
    DataFrame,
    RuntimeConfig,
    SessionConfig,
    SessionContext,
    SQLOptions,
    column,
    literal,
)


def test_create_context_no_args():
    SessionContext()


def test_create_context_session_config_only():
    SessionContext(config=SessionConfig())


def test_create_context_runtime_config_only():
    SessionContext(runtime=RuntimeConfig())


@pytest.mark.parametrize("path_to_str", (True, False))
def test_runtime_configs(tmp_path, path_to_str):
    path1 = tmp_path / "dir1"
    path2 = tmp_path / "dir2"

    path1 = str(path1) if path_to_str else path1
    path2 = str(path2) if path_to_str else path2

    runtime = RuntimeConfig().with_disk_manager_specified(path1, path2)
    config = SessionConfig().with_default_catalog_and_schema("foo", "bar")
    ctx = SessionContext(config, runtime)
    assert ctx is not None

    db = ctx.catalog("foo").database("bar")
    assert db is not None


@pytest.mark.parametrize("path_to_str", (True, False))
def test_temporary_files(tmp_path, path_to_str):
    path = str(tmp_path) if path_to_str else tmp_path

    runtime = RuntimeConfig().with_temp_file_path(path)
    config = SessionConfig().with_default_catalog_and_schema("foo", "bar")
    ctx = SessionContext(config, runtime)
    assert ctx is not None

    db = ctx.catalog("foo").database("bar")
    assert db is not None


def test_create_context_with_all_valid_args():
    runtime = RuntimeConfig().with_disk_manager_os().with_fair_spill_pool(10000000)
    config = (
        SessionConfig()
        .with_create_default_catalog_and_schema(True)
        .with_default_catalog_and_schema("foo", "bar")
        .with_target_partitions(1)
        .with_information_schema(True)
        .with_repartition_joins(False)
        .with_repartition_aggregations(False)
        .with_repartition_windows(False)
        .with_parquet_pruning(False)
    )

    ctx = SessionContext(config, runtime)

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

    assert ctx.catalog().database().names() == {"t"}

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
    tables = list(ctx.catalog().database().names())

    assert df
    assert len(tables) == 1
    assert len(tables[0]) == 33
    assert tables[0].startswith("c")
    # ensure that the rest of the table name contains
    # only hexadecimal numbers
    for c in tables[0][1:]:
        assert c in "0123456789abcdef"


def test_create_dataframe_registers_with_defined_table_name(ctx):
    # create a RecordBatch and register it as memtable
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )

    df = ctx.create_dataframe([[batch]], name="tbl")
    tables = list(ctx.catalog().database().names())

    assert df
    assert len(tables) == 1
    assert tables[0] == "tbl"


def test_from_arrow_table(ctx):
    # create a PyArrow table
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    table = pa.Table.from_pydict(data)

    # convert to DataFrame
    df = ctx.from_arrow(table)
    tables = list(ctx.catalog().database().names())

    assert df
    assert len(tables) == 1
    assert isinstance(df, DataFrame)
    assert set(df.schema().names) == {"a", "b"}
    assert df.collect()[0].num_rows == 3


def record_batch_generator(num_batches: int):
    schema = pa.schema([("a", pa.int64()), ("b", pa.int64())])
    for i in range(num_batches):
        yield pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array([4, 5, 6])], schema=schema
        )


@pytest.mark.parametrize(
    "source",
    [
        # __arrow_c_array__ sources
        pa.array([{"a": 1, "b": 4}, {"a": 2, "b": 5}, {"a": 3, "b": 6}]),
        # __arrow_c_stream__ sources
        pa.RecordBatch.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]}),
        pa.RecordBatchReader.from_batches(
            pa.schema([("a", pa.int64()), ("b", pa.int64())]), record_batch_generator(1)
        ),
        pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]}),
    ],
)
def test_from_arrow_sources(ctx, source) -> None:
    df = ctx.from_arrow(source)
    assert df
    assert isinstance(df, DataFrame)
    assert df.schema().names == ["a", "b"]
    assert df.count() == 3


def test_from_arrow_table_with_name(ctx):
    # create a PyArrow table
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    table = pa.Table.from_pydict(data)

    # convert to DataFrame with optional name
    df = ctx.from_arrow(table, name="tbl")
    tables = list(ctx.catalog().database().names())

    assert df
    assert tables[0] == "tbl"


def test_from_arrow_table_empty(ctx):
    data = {"a": [], "b": []}
    schema = pa.schema([("a", pa.int32()), ("b", pa.string())])
    table = pa.Table.from_pydict(data, schema=schema)

    # convert to DataFrame
    df = ctx.from_arrow(table)
    tables = list(ctx.catalog().database().names())

    assert df
    assert len(tables) == 1
    assert isinstance(df, DataFrame)
    assert set(df.schema().names) == {"a", "b"}
    assert len(df.collect()) == 0


def test_from_arrow_table_empty_no_schema(ctx):
    data = {"a": [], "b": []}
    table = pa.Table.from_pydict(data)

    # convert to DataFrame
    df = ctx.from_arrow(table)
    tables = list(ctx.catalog().database().names())

    assert df
    assert len(tables) == 1
    assert isinstance(df, DataFrame)
    assert set(df.schema().names) == {"a", "b"}
    assert len(df.collect()) == 0


def test_from_pylist(ctx):
    # create a dataframe from Python list
    data = [
        {"a": 1, "b": 4},
        {"a": 2, "b": 5},
        {"a": 3, "b": 6},
    ]

    df = ctx.from_pylist(data)
    tables = list(ctx.catalog().database().names())

    assert df
    assert len(tables) == 1
    assert isinstance(df, DataFrame)
    assert set(df.schema().names) == {"a", "b"}
    assert df.collect()[0].num_rows == 3


def test_from_pydict(ctx):
    # create a dataframe from Python dictionary
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}

    df = ctx.from_pydict(data)
    tables = list(ctx.catalog().database().names())

    assert df
    assert len(tables) == 1
    assert isinstance(df, DataFrame)
    assert set(df.schema().names) == {"a", "b"}
    assert df.collect()[0].num_rows == 3


def test_from_pandas(ctx):
    # create a dataframe from pandas dataframe
    pd = pytest.importorskip("pandas")
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    pandas_df = pd.DataFrame(data)

    df = ctx.from_pandas(pandas_df)
    tables = list(ctx.catalog().database().names())

    assert df
    assert len(tables) == 1
    assert isinstance(df, DataFrame)
    assert set(df.schema().names) == {"a", "b"}
    assert df.collect()[0].num_rows == 3


def test_from_polars(ctx):
    # create a dataframe from Polars dataframe
    pd = pytest.importorskip("polars")
    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    polars_df = pd.DataFrame(data)

    df = ctx.from_polars(polars_df)
    tables = list(ctx.catalog().database().names())

    assert df
    assert len(tables) == 1
    assert isinstance(df, DataFrame)
    assert set(df.schema().names) == {"a", "b"}
    assert df.collect()[0].num_rows == 3


def test_register_table(ctx, database):
    default = ctx.catalog()
    public = default.database("public")
    assert public.names() == {"csv", "csv1", "csv2"}
    table = public.table("csv")

    ctx.register_table("csv3", table)
    assert public.names() == {"csv", "csv1", "csv2", "csv3"}


def test_read_table(ctx, database):
    default = ctx.catalog()
    public = default.database("public")
    assert public.names() == {"csv", "csv1", "csv2"}

    table = public.table("csv")
    table_df = ctx.read_table(table)
    table_df.show()


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

    assert ctx.catalog().database().names() == {"t"}

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

    assert ctx.catalog().database().names() == {"t"}
    df = ctx.sql("SELECT a+b, a-b FROM t WHERE a BETWEEN 2 and 3 AND b > 5")

    # Make sure the filter was pushed down in Physical Plan
    df.explain()
    captured = capfd.readouterr()
    assert "filter_expr=(((a >= 2) and (a <= 3)) and (b > 5))" in captured.out

    result = df.collect()

    assert result[0].column(0) == pa.array([9])
    assert result[0].column(1) == pa.array([-3])


def test_pyarrow_predicate_pushdown_is_null(ctx, capfd):
    """Ensure that pyarrow filter gets pushed down for `IsNull`"""
    # create a RecordBatch and register it as a pyarrow.dataset.Dataset
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6]), pa.array([7, None, 9])],
        names=["a", "b", "c"],
    )
    dataset = ds.dataset([batch])
    ctx.register_dataset("t", dataset)
    # Make sure the filter was pushed down in Physical Plan
    df = ctx.sql("SELECT a FROM t WHERE c is NULL")
    df.explain()
    captured = capfd.readouterr()
    assert "filter_expr=is_null(c, {nan_is_null=false})" in captured.out

    result = df.collect()
    assert result[0].column(0) == pa.array([2])


def test_pyarrow_predicate_pushdown_timestamp(ctx, tmpdir, capfd):
    """Ensure that pyarrow filter gets pushed down for timestamp"""
    # Ref: https://github.com/apache/datafusion-python/issues/703

    # create pyarrow dataset with no actual files
    col_type = pa.timestamp("ns", "+00:00")
    nyd_2000 = pa.scalar(dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc), col_type)
    pa_dataset_fs = pa.fs.SubTreeFileSystem(str(tmpdir), pa.fs.LocalFileSystem())
    pa_dataset_format = pa.dataset.ParquetFileFormat()
    pa_dataset_partition = pa.dataset.field("a") <= nyd_2000
    fragments = [
        # NOTE: we never actually make this file.
        # Working predicate pushdown means it never gets accessed
        pa_dataset_format.make_fragment(
            "1.parquet",
            filesystem=pa_dataset_fs,
            partition_expression=pa_dataset_partition,
        )
    ]
    pa_dataset = pa.dataset.FileSystemDataset(
        fragments,
        pa.schema([pa.field("a", col_type)]),
        pa_dataset_format,
        pa_dataset_fs,
    )

    ctx.register_dataset("t", pa_dataset)

    # the partition for our only fragment is for a < 2000-01-01.
    # so querying for a > 2024-01-01 should not touch any files
    df = ctx.sql("SELECT * FROM t WHERE a > '2024-01-01T00:00:00+00:00'")
    assert df.collect() == []


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

    assert ctx.catalog().database().names() == {"t"}

    df = ctx.table("t")

    # This filter will not be pushed down to DatasetExec since it
    # isn't supported
    df = df.filter(column("nested_data")["b"] > literal(5)).select(
        column("nested_data")["a"] + column("nested_data")["b"],
        column("nested_data")["a"] - column("nested_data")["b"],
    )

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


def test_table_not_found(ctx):
    from uuid import uuid4

    with pytest.raises(KeyError):
        ctx.table(f"not-found-{uuid4()}")


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


def test_read_json_compressed(ctx, tmp_path):
    path = os.path.dirname(os.path.abspath(__file__))
    test_data_path = os.path.join(path, "data_test_context", "data.json")

    # File compression type
    gzip_path = tmp_path / "data.json.gz"

    with open(test_data_path, "rb") as csv_file:
        with gzip.open(gzip_path, "wb") as gzipped_file:
            gzipped_file.writelines(csv_file)

    df = ctx.read_json(gzip_path, file_extension=".gz", file_compression_type="gz")
    result = df.collect()

    assert result[0].column(0) == pa.array(["a", "b", "c"])
    assert result[0].column(1) == pa.array([1, 2, 3])


def test_read_csv(ctx):
    csv_df = ctx.read_csv(path="testing/data/csv/aggregate_test_100.csv")
    csv_df.select(column("c1")).show()


def test_read_csv_list(ctx):
    csv_df = ctx.read_csv(path=["testing/data/csv/aggregate_test_100.csv"])
    expected = csv_df.count() * 2

    double_csv_df = ctx.read_csv(
        path=[
            "testing/data/csv/aggregate_test_100.csv",
            "testing/data/csv/aggregate_test_100.csv",
        ]
    )
    actual = double_csv_df.count()

    double_csv_df.select(column("c1")).show()
    assert actual == expected


def test_read_csv_compressed(ctx, tmp_path):
    test_data_path = "testing/data/csv/aggregate_test_100.csv"

    # File compression type
    gzip_path = tmp_path / "aggregate_test_100.csv.gz"

    with open(test_data_path, "rb") as csv_file:
        with gzip.open(gzip_path, "wb") as gzipped_file:
            gzipped_file.writelines(csv_file)

    csv_df = ctx.read_csv(gzip_path, file_extension=".gz", file_compression_type="gz")
    csv_df.select(column("c1")).show()


def test_read_parquet(ctx):
    parquet_df = ctx.read_parquet(path="parquet/data/alltypes_plain.parquet")
    parquet_df.show()
    assert parquet_df is not None

    path = pathlib.Path.cwd() / "parquet/data/alltypes_plain.parquet"
    parquet_df = ctx.read_parquet(path=path)
    assert parquet_df is not None


def test_read_avro(ctx):
    avro_df = ctx.read_avro(path="testing/data/avro/alltypes_plain.avro")
    avro_df.show()
    assert avro_df is not None

    path = pathlib.Path.cwd() / "testing/data/avro/alltypes_plain.avro"
    avro_df = ctx.read_avro(path=path)
    assert avro_df is not None


def test_create_sql_options():
    SQLOptions()


def test_sql_with_options_no_ddl(ctx):
    sql = "CREATE TABLE IF NOT EXISTS valuetable AS VALUES(1,'HELLO'),(12,'DATAFUSION')"
    ctx.sql(sql)
    options = SQLOptions().with_allow_ddl(False)
    with pytest.raises(Exception, match="DDL"):
        ctx.sql_with_options(sql, options=options)


def test_sql_with_options_no_dml(ctx):
    table_name = "t"
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    dataset = ds.dataset([batch])
    ctx.register_dataset(table_name, dataset)
    sql = f'INSERT INTO "{table_name}" VALUES (1, 2), (2, 3);'
    ctx.sql(sql)
    options = SQLOptions().with_allow_dml(False)
    with pytest.raises(Exception, match="DML"):
        ctx.sql_with_options(sql, options=options)


def test_sql_with_options_no_statements(ctx):
    sql = "SET time zone = 1;"
    ctx.sql(sql)
    options = SQLOptions().with_allow_statements(False)
    with pytest.raises(Exception, match="SetVariable"):
        ctx.sql_with_options(sql, options=options)
