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

import numpy as np
import pyarrow as pa
from pyarrow.csv import write_csv
import pyarrow.dataset as ds
import pytest
from datafusion.object_store import LocalFileSystem

from datafusion import udf, col

from . import generic as helpers


def test_no_table(ctx):
    with pytest.raises(Exception, match="DataFusion error"):
        ctx.sql("SELECT a FROM b").collect()


def test_register_csv(ctx, tmp_path):
    path = tmp_path / "test.csv"
    gzip_path = tmp_path / "test.csv.gz"

    table = pa.Table.from_arrays(
        [
            [1, 2, 3, 4],
            ["a", "b", "c", "d"],
            [1.1, 2.2, 3.3, 4.4],
        ],
        names=["int", "str", "float"],
    )
    write_csv(table, path)

    with open(path, "rb") as csv_file:
        with gzip.open(gzip_path, "wb") as gzipped_file:
            gzipped_file.writelines(csv_file)

    ctx.register_csv("csv", path)
    ctx.register_csv("csv1", str(path))
    ctx.register_csv(
        "csv2",
        path,
        has_header=True,
        delimiter=",",
        schema_infer_max_records=10,
    )
    ctx.register_csv(
        "csv_gzip",
        gzip_path,
        file_extension="gz",
        file_compression_type="gzip",
    )

    alternative_schema = pa.schema(
        [
            ("some_int", pa.int16()),
            ("some_bytes", pa.string()),
            ("some_floats", pa.float32()),
        ]
    )
    ctx.register_csv("csv3", path, schema=alternative_schema)

    assert ctx.catalog().database().names() == {
        "csv",
        "csv1",
        "csv2",
        "csv3",
        "csv_gzip",
    }

    for table in ["csv", "csv1", "csv2", "csv_gzip"]:
        result = ctx.sql(f"SELECT COUNT(int) AS cnt FROM {table}").collect()
        result = pa.Table.from_batches(result)
        assert result.to_pydict() == {"cnt": [4]}

    result = ctx.sql("SELECT * FROM csv3").collect()
    result = pa.Table.from_batches(result)
    assert result.schema == alternative_schema

    with pytest.raises(ValueError, match="Delimiter must be a single character"):
        ctx.register_csv("csv4", path, delimiter="wrong")

    with pytest.raises(
        ValueError,
        match="file_compression_type must one of: gzip, bz2, xz, zstd",
    ):
        ctx.register_csv("csv4", path, file_compression_type="rar")


def test_register_parquet(ctx, tmp_path):
    path = helpers.write_parquet(tmp_path / "a.parquet", helpers.data())
    ctx.register_parquet("t", path)
    ctx.register_parquet("t1", str(path))
    assert ctx.catalog().database().names() == {"t", "t1"}

    result = ctx.sql("SELECT COUNT(a) AS cnt FROM t").collect()
    result = pa.Table.from_batches(result)
    assert result.to_pydict() == {"cnt": [100]}


@pytest.mark.parametrize("path_to_str", (True, False))
def test_register_parquet_partitioned(ctx, tmp_path, path_to_str):
    dir_root = tmp_path / "dataset_parquet_partitioned"
    dir_root.mkdir(exist_ok=False)
    (dir_root / "grp=a").mkdir(exist_ok=False)
    (dir_root / "grp=b").mkdir(exist_ok=False)

    table = pa.Table.from_arrays(
        [
            [1, 2, 3, 4],
            ["a", "b", "c", "d"],
            [1.1, 2.2, 3.3, 4.4],
        ],
        names=["int", "str", "float"],
    )
    pa.parquet.write_table(table.slice(0, 3), dir_root / "grp=a/file.parquet")
    pa.parquet.write_table(table.slice(3, 4), dir_root / "grp=b/file.parquet")

    dir_root = str(dir_root) if path_to_str else dir_root

    ctx.register_parquet(
        "datapp",
        dir_root,
        table_partition_cols=[("grp", "string")],
        parquet_pruning=True,
        file_extension=".parquet",
    )
    assert ctx.catalog().database().names() == {"datapp"}

    result = ctx.sql("SELECT grp, COUNT(*) AS cnt FROM datapp GROUP BY grp").collect()
    result = pa.Table.from_batches(result)

    rd = result.to_pydict()
    assert dict(zip(rd["grp"], rd["cnt"])) == {"a": 3, "b": 1}


@pytest.mark.parametrize("path_to_str", (True, False))
def test_register_dataset(ctx, tmp_path, path_to_str):
    path = helpers.write_parquet(tmp_path / "a.parquet", helpers.data())
    path = str(path) if path_to_str else path
    dataset = ds.dataset(path, format="parquet")

    ctx.register_dataset("t", dataset)
    assert ctx.catalog().database().names() == {"t"}

    result = ctx.sql("SELECT COUNT(a) AS cnt FROM t").collect()
    result = pa.Table.from_batches(result)
    assert result.to_pydict() == {"cnt": [100]}


def test_register_json(ctx, tmp_path):
    path = os.path.dirname(os.path.abspath(__file__))
    test_data_path = os.path.join(path, "data_test_context", "data.json")
    gzip_path = tmp_path / "data.json.gz"

    with open(test_data_path, "rb") as json_file:
        with gzip.open(gzip_path, "wb") as gzipped_file:
            gzipped_file.writelines(json_file)

    ctx.register_json("json", test_data_path)
    ctx.register_json("json1", str(test_data_path))
    ctx.register_json(
        "json2",
        test_data_path,
        schema_infer_max_records=10,
    )
    ctx.register_json(
        "json_gzip",
        gzip_path,
        file_extension="gz",
        file_compression_type="gzip",
    )
    ctx.register_json(
        "json_gzip1",
        str(gzip_path),
        file_extension="gz",
        file_compression_type="gzip",
    )

    alternative_schema = pa.schema(
        [
            ("some_int", pa.int16()),
            ("some_bytes", pa.string()),
            ("some_floats", pa.float32()),
        ]
    )
    ctx.register_json("json3", path, schema=alternative_schema)

    assert ctx.catalog().database().names() == {
        "json",
        "json1",
        "json2",
        "json3",
        "json_gzip",
        "json_gzip1",
    }

    for table in ["json", "json1", "json2", "json_gzip"]:
        result = ctx.sql(f'SELECT COUNT("B") AS cnt FROM {table}').collect()
        result = pa.Table.from_batches(result)
        assert result.to_pydict() == {"cnt": [3]}

    result = ctx.sql("SELECT * FROM json3").collect()
    result = pa.Table.from_batches(result, alternative_schema)
    assert result.schema == alternative_schema

    with pytest.raises(
        ValueError,
        match="file_compression_type must one of: gzip, bz2, xz, zstd",
    ):
        ctx.register_json("json4", gzip_path, file_compression_type="rar")


def test_register_avro(ctx):
    path = "testing/data/avro/alltypes_plain.avro"
    ctx.register_avro("alltypes_plain", path)
    result = ctx.sql(
        "SELECT SUM(tinyint_col) as tinyint_sum FROM alltypes_plain"
    ).collect()
    result = pa.Table.from_batches(result).to_pydict()
    assert result["tinyint_sum"][0] > 0

    alternative_schema = pa.schema(
        [
            pa.field("id", pa.int64()),
        ]
    )

    ctx.register_avro(
        "alltypes_plain_schema",
        path,
        schema=alternative_schema,
    )
    result = ctx.sql("SELECT * FROM alltypes_plain_schema").collect()
    result = pa.Table.from_batches(result)
    assert result.schema == alternative_schema


def test_execute(ctx, tmp_path):
    data = [1, 1, 2, 2, 3, 11, 12]

    # single column, "a"
    path = helpers.write_parquet(tmp_path / "a.parquet", pa.array(data))
    ctx.register_parquet("t", path)

    assert ctx.catalog().database().names() == {"t"}

    # count
    result = ctx.sql("SELECT COUNT(a) AS cnt FROM t WHERE a IS NOT NULL").collect()

    expected = pa.array([7], pa.int64())
    expected = [pa.RecordBatch.from_arrays([expected], ["cnt"])]
    assert result == expected

    # where
    expected = pa.array([2], pa.int64())
    expected = [pa.RecordBatch.from_arrays([expected], ["cnt"])]
    result = ctx.sql("SELECT COUNT(a) AS cnt FROM t WHERE a > 10").collect()
    assert result == expected

    # group by
    results = ctx.sql(
        "SELECT CAST(a as int) AS a, COUNT(a) AS cnt FROM t GROUP BY a"
    ).collect()

    # group by returns batches
    result_keys = []
    result_values = []
    for result in results:
        pydict = result.to_pydict()
        result_keys.extend(pydict["a"])
        result_values.extend(pydict["cnt"])

    result_keys, result_values = (
        list(t) for t in zip(*sorted(zip(result_keys, result_values)))
    )

    assert result_keys == [1, 2, 3, 11, 12]
    assert result_values == [2, 2, 1, 1, 1]

    # order by
    result = ctx.sql(
        "SELECT a, CAST(a AS int) AS a_int FROM t ORDER BY a DESC LIMIT 2"
    ).collect()
    expected_a = pa.array([50.0219, 50.0152], pa.float64())
    expected_cast = pa.array([50, 50], pa.int32())
    expected = [pa.RecordBatch.from_arrays([expected_a, expected_cast], ["a", "a_int"])]
    np.testing.assert_equal(expected[0].column(1), expected[0].column(1))


def test_cast(ctx, tmp_path):
    """Verify that we can cast"""
    path = helpers.write_parquet(tmp_path / "a.parquet", helpers.data())
    ctx.register_parquet("t", path)

    valid_types = [
        "smallint",
        "int",
        "bigint",
        "float(32)",
        "float(64)",
        "float",
    ]

    select = ", ".join([f"CAST(9 AS {t}) AS A{i}" for i, t in enumerate(valid_types)])

    # can execute, which implies that we can cast
    ctx.sql(f"SELECT {select} FROM t").collect()


@pytest.mark.parametrize(
    ("fn", "input_types", "output_type", "input_values", "expected_values"),
    [
        (
            lambda x: x,
            [pa.float64()],
            pa.float64(),
            [-1.2, None, 1.2],
            [-1.2, None, 1.2],
        ),
        (
            lambda x: x.is_null(),
            [pa.float64()],
            pa.bool_(),
            [-1.2, None, 1.2],
            [False, True, False],
        ),
    ],
)
def test_udf(
    ctx, tmp_path, fn, input_types, output_type, input_values, expected_values
):
    # write to disk
    path = helpers.write_parquet(tmp_path / "a.parquet", pa.array(input_values))
    ctx.register_parquet("t", path)

    func = udf(fn, input_types, output_type, name="func", volatility="immutable")
    ctx.register_udf(func)

    batches = ctx.sql("SELECT func(a) AS tt FROM t").collect()
    result = batches[0].column(0)

    assert result == pa.array(expected_values)


_null_mask = np.array([False, True, False])


@pytest.mark.parametrize(
    "arr",
    [
        pa.array(["a", "b", "c"], pa.utf8(), _null_mask),
        pa.array(["a", "b", "c"], pa.large_utf8(), _null_mask),
        pa.array([b"1", b"2", b"3"], pa.binary(), _null_mask),
        pa.array([b"1111", b"2222", b"3333"], pa.large_binary(), _null_mask),
        pa.array([False, True, True], None, _null_mask),
        pa.array([0, 1, 2], None),
        helpers.data_binary_other(),
        helpers.data_date32(),
        helpers.data_with_nans(),
        # C data interface missing
        pytest.param(
            pa.array([b"1111", b"2222", b"3333"], pa.binary(4), _null_mask),
            id="binary4",
            marks=pytest.mark.xfail,
        ),
        pytest.param(
            helpers.data_datetime("s"), id="datetime_s", marks=pytest.mark.xfail
        ),
        pytest.param(
            helpers.data_datetime("ms"), id="datetime_ms", marks=pytest.mark.xfail
        ),
        pytest.param(
            helpers.data_datetime("us"), id="datetime_us", marks=pytest.mark.xfail
        ),
        pytest.param(
            helpers.data_datetime("ns"), id="datetime_ns", marks=pytest.mark.xfail
        ),
        # Not writtable to parquet
        pytest.param(
            helpers.data_timedelta("s"), id="timedelta_s", marks=pytest.mark.xfail
        ),
        pytest.param(
            helpers.data_timedelta("ms"), id="timedelta_ms", marks=pytest.mark.xfail
        ),
        pytest.param(
            helpers.data_timedelta("us"), id="timedelta_us", marks=pytest.mark.xfail
        ),
        pytest.param(
            helpers.data_timedelta("ns"), id="timedelta_ns", marks=pytest.mark.xfail
        ),
    ],
)
def test_simple_select(ctx, tmp_path, arr):
    path = helpers.write_parquet(tmp_path / "a.parquet", arr)
    ctx.register_parquet("t", path)

    batches = ctx.sql("SELECT a AS tt FROM t").collect()
    result = batches[0].column(0)

    np.testing.assert_equal(result, arr)


@pytest.mark.parametrize("file_sort_order", (None, [[col("int").sort(True, True)]]))
@pytest.mark.parametrize("pass_schema", (True, False))
@pytest.mark.parametrize("path_to_str", (True, False))
def test_register_listing_table(
    ctx, tmp_path, pass_schema, file_sort_order, path_to_str
):
    dir_root = tmp_path / "dataset_parquet_partitioned"
    dir_root.mkdir(exist_ok=False)
    (dir_root / "grp=a/date_id=20201005").mkdir(exist_ok=False, parents=True)
    (dir_root / "grp=a/date_id=20211005").mkdir(exist_ok=False, parents=True)
    (dir_root / "grp=b/date_id=20201005").mkdir(exist_ok=False, parents=True)

    table = pa.Table.from_arrays(
        [
            [1, 2, 3, 4, 5, 6, 7],
            ["a", "b", "c", "d", "e", "f", "g"],
            [1.1, 2.2, 3.3, 4.4, 5.5, 6.6, 7.7],
        ],
        names=["int", "str", "float"],
    )
    pa.parquet.write_table(
        table.slice(0, 3), dir_root / "grp=a/date_id=20201005/file.parquet"
    )
    pa.parquet.write_table(
        table.slice(3, 2), dir_root / "grp=a/date_id=20211005/file.parquet"
    )
    pa.parquet.write_table(
        table.slice(5, 10), dir_root / "grp=b/date_id=20201005/file.parquet"
    )

    dir_root = f"file://{dir_root}/" if path_to_str else dir_root

    ctx.register_object_store("file://local", LocalFileSystem(), None)
    ctx.register_listing_table(
        "my_table",
        dir_root,
        table_partition_cols=[("grp", "string"), ("date_id", "int")],
        file_extension=".parquet",
        schema=table.schema if pass_schema else None,
        file_sort_order=file_sort_order,
    )
    assert ctx.catalog().database().names() == {"my_table"}

    result = ctx.sql(
        "SELECT grp, COUNT(*) AS count FROM my_table GROUP BY grp"
    ).collect()
    result = pa.Table.from_batches(result)

    rd = result.to_pydict()
    assert dict(zip(rd["grp"], rd["count"])) == {"a": 5, "b": 2}

    result = ctx.sql(
        "SELECT grp, COUNT(*) AS count FROM my_table WHERE date_id=20201005 GROUP BY grp"
    ).collect()
    result = pa.Table.from_batches(result)

    rd = result.to_pydict()
    assert dict(zip(rd["grp"], rd["count"])) == {"a": 3, "b": 2}
