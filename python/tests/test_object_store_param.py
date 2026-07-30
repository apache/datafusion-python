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

"""Tests for the object_store parameter on register/read file methods."""

import contextlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from datafusion import SessionContext
from datafusion.object_store import LocalFileSystem


@pytest.fixture
def ctx():
    return SessionContext()


@pytest.mark.parametrize(
    ("path", "scheme", "host"),
    [
        ("s3://my-bucket/path/file.parquet", "s3://", "my-bucket"),
        ("gs://my-gcs-bucket/data.parquet", "gs://", "my-gcs-bucket"),
        ("az://my-container/data.parquet", "az://", "my-container"),
        ("https://example.com/data.parquet", "https://", "example.com"),
        ("file:///tmp/data.parquet", "file://", None),
    ],
)
def test_register_object_store_for_url(ctx, path, scheme, host):
    store = MagicMock()

    with patch.object(ctx, "register_object_store") as register:
        ctx._register_object_store_for_path(path, store)

    register.assert_called_once_with(scheme, store, host=host)


@pytest.mark.parametrize(
    ("path", "error"),
    [
        ("/local/path/file.parquet", "Cannot determine object store URL"),
        ("relative/path.parquet", "Cannot determine object store URL"),
        (Path("/local/file.parquet"), "Cannot determine object store URL"),
        ("C:\\Users\\data\\file.parquet", "must include a host or bucket"),
        ("s3:///key.parquet", "must include a host or bucket"),
    ],
)
def test_register_object_store_rejects_invalid_url(path, error, ctx):
    with pytest.raises(ValueError, match=error):
        ctx._register_object_store_for_path(path, MagicMock())


@pytest.mark.parametrize(
    ("method_name", "args", "path"),
    [
        ("register_parquet", ("table",), "s3://bucket/data.parquet"),
        ("read_parquet", (), "s3://bucket/data.parquet"),
        ("register_csv", ("table",), "s3://bucket/data.csv"),
        ("read_csv", (), "s3://bucket/data.csv"),
        ("register_json", ("table",), "s3://bucket/data.json"),
        ("read_json", (), "s3://bucket/data.json"),
        ("register_avro", ("table",), "s3://bucket/data.avro"),
        ("read_avro", (), "s3://bucket/data.avro"),
        ("register_arrow", ("table",), "s3://bucket/data.arrow"),
        ("read_arrow", (), "s3://bucket/data.arrow"),
    ],
)
def test_file_methods_register_object_store(ctx, method_name, args, path):
    store = MagicMock()

    # The remote file does not exist. Registration happens before DataFusion
    # tries to inspect it, which is the behavior under test.
    with (
        patch.object(ctx, "register_object_store") as register,
        contextlib.suppress(Exception),
    ):
        getattr(ctx, method_name)(*args, path, object_store=store)

    register.assert_called_once_with("s3://", store, host="bucket")


def test_register_csv_uses_first_path_for_object_store(ctx):
    store = MagicMock()
    paths = ["s3://bucket/a.csv", "s3://bucket/b.csv"]

    with (
        patch.object(ctx, "register_object_store") as register,
        contextlib.suppress(Exception),
    ):
        ctx.register_csv("table", paths, object_store=store)

    register.assert_called_once_with("s3://", store, host="bucket")


def test_object_store_none_does_not_register(ctx):
    with (
        patch.object(ctx, "register_object_store") as register,
        contextlib.suppress(Exception),
    ):
        ctx.register_parquet("table", "missing.parquet")

    register.assert_not_called()


def test_file_method_rejects_local_path_with_object_store(ctx):
    with pytest.raises(ValueError, match="Cannot determine object store URL"):
        ctx.register_parquet("table", "/local/file.parquet", object_store=MagicMock())


@pytest.mark.parametrize("method_name", ["register_parquet", "read_parquet"])
def test_parquet_methods_with_local_object_store(ctx, tmp_path, method_name):
    table = pa.table({"value": [10, 20, 30]})
    parquet_path = tmp_path / "data.parquet"
    pq.write_table(table, parquet_path)

    path = parquet_path.as_uri()
    if method_name == "register_parquet":
        ctx.register_parquet("test_table", path, object_store=LocalFileSystem())
        dataframe = ctx.sql("SELECT * FROM test_table")
    else:
        dataframe = ctx.read_parquet(path, object_store=LocalFileSystem())

    assert dataframe.collect()[0].column("value").to_pylist() == [10, 20, 30]
