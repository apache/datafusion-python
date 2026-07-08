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
import pytest
from datafusion import SessionContext


@pytest.fixture
def ctx():
    return SessionContext()


class TestRegisterObjectStoreForPath:
    """Unit tests for _register_object_store_for_path URL parsing logic."""

    def test_parses_s3_url(self, ctx):
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            ctx._register_object_store_for_path(
                "s3://my-bucket/path/to/file.parquet", mock_store
            )
            mock_register.assert_called_once_with("s3://", mock_store, host="my-bucket")

    def test_parses_gs_url(self, ctx):
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            ctx._register_object_store_for_path(
                "gs://my-gcs-bucket/data.parquet", mock_store
            )
            mock_register.assert_called_once_with(
                "gs://", mock_store, host="my-gcs-bucket"
            )

    def test_parses_az_url(self, ctx):
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            ctx._register_object_store_for_path(
                "az://my-container/data.parquet", mock_store
            )
            mock_register.assert_called_once_with(
                "az://", mock_store, host="my-container"
            )

    def test_parses_https_url(self, ctx):
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            ctx._register_object_store_for_path(
                "https://my-host.example.com/data.parquet", mock_store
            )
            mock_register.assert_called_once_with(
                "https://", mock_store, host="my-host.example.com"
            )

    def test_raises_on_local_path(self, ctx):
        mock_store = MagicMock()
        with pytest.raises(ValueError, match="Cannot determine object store URL"):
            ctx._register_object_store_for_path("/local/path/file.parquet", mock_store)

    def test_raises_on_relative_path(self, ctx):
        mock_store = MagicMock()
        with pytest.raises(ValueError, match="Cannot determine object store URL"):
            ctx._register_object_store_for_path("relative/path.parquet", mock_store)

    def test_raises_on_windows_path(self, ctx):
        mock_store = MagicMock()
        with pytest.raises(ValueError, match="Cannot determine object store URL"):
            ctx._register_object_store_for_path(
                "C:\\Users\\data\\file.parquet", mock_store
            )

    def test_accepts_pathlib_path_raises(self, ctx):
        """pathlib.Path cannot represent URLs, so this should raise."""
        mock_store = MagicMock()
        # pathlib.Path strips the scheme, so this becomes a local path
        with pytest.raises(ValueError, match="Cannot determine object store URL"):
            ctx._register_object_store_for_path(Path("/local/file.parquet"), mock_store)


class TestRegisterParquetObjectStore:
    """Tests for register_parquet with object_store parameter."""

    def test_object_store_none_does_not_register(self, ctx):
        """When object_store is None, register_object_store is not called."""
        with patch.object(ctx, "register_object_store") as mock_register:
            # This will fail at the Rust level (file doesn't exist), but
            # we're testing that register_object_store is NOT called
            with contextlib.suppress(Exception):
                ctx.register_parquet("t", "s3://bucket/file.parquet")
            mock_register.assert_not_called()

    def test_object_store_triggers_registration(self, ctx):
        """When object_store is provided, register_object_store is called."""
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            with contextlib.suppress(Exception):
                ctx.register_parquet(
                    "t",
                    "s3://my-bucket/file.parquet",
                    object_store=mock_store,
                )
            mock_register.assert_called_once_with("s3://", mock_store, host="my-bucket")

    def test_object_store_invalid_path_raises(self, ctx):
        """Providing object_store with a local path raises ValueError."""
        mock_store = MagicMock()
        with pytest.raises(ValueError, match="Cannot determine object store URL"):
            ctx.register_parquet("t", "/local/file.parquet", object_store=mock_store)


class TestReadParquetObjectStore:
    """Tests for read_parquet with object_store parameter."""

    def test_object_store_triggers_registration(self, ctx):
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            with contextlib.suppress(Exception):
                ctx.read_parquet("s3://my-bucket/file.parquet", object_store=mock_store)
            mock_register.assert_called_once_with("s3://", mock_store, host="my-bucket")


class TestRegisterCsvObjectStore:
    """Tests for register_csv with object_store parameter."""

    def test_object_store_triggers_registration(self, ctx):
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            with contextlib.suppress(Exception):
                ctx.register_csv(
                    "t", "s3://my-bucket/data.csv", object_store=mock_store
                )
            mock_register.assert_called_once_with("s3://", mock_store, host="my-bucket")

    def test_object_store_with_list_path(self, ctx):
        """For list paths, the first entry is used for URL parsing."""
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            with contextlib.suppress(Exception):
                ctx.register_csv(
                    "t",
                    ["s3://my-bucket/a.csv", "s3://my-bucket/b.csv"],
                    object_store=mock_store,
                )
            mock_register.assert_called_once_with("s3://", mock_store, host="my-bucket")


class TestReadCsvObjectStore:
    """Tests for read_csv with object_store parameter."""

    def test_object_store_triggers_registration(self, ctx):
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            with contextlib.suppress(Exception):
                ctx.read_csv("gs://bucket/data.csv", object_store=mock_store)
            mock_register.assert_called_once_with("gs://", mock_store, host="bucket")


class TestRegisterJsonObjectStore:
    """Tests for register_json with object_store parameter."""

    def test_object_store_triggers_registration(self, ctx):
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            with contextlib.suppress(Exception):
                ctx.register_json("t", "s3://bucket/data.json", object_store=mock_store)
            mock_register.assert_called_once_with("s3://", mock_store, host="bucket")


class TestReadJsonObjectStore:
    """Tests for read_json with object_store parameter."""

    def test_object_store_triggers_registration(self, ctx):
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            with contextlib.suppress(Exception):
                ctx.read_json("s3://bucket/data.json", object_store=mock_store)
            mock_register.assert_called_once_with("s3://", mock_store, host="bucket")


class TestRegisterAvroObjectStore:
    """Tests for register_avro with object_store parameter."""

    def test_object_store_triggers_registration(self, ctx):
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            with contextlib.suppress(Exception):
                ctx.register_avro("t", "s3://bucket/data.avro", object_store=mock_store)
            mock_register.assert_called_once_with("s3://", mock_store, host="bucket")


class TestReadAvroObjectStore:
    """Tests for read_avro with object_store parameter."""

    def test_object_store_triggers_registration(self, ctx):
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            with contextlib.suppress(Exception):
                ctx.read_avro("s3://bucket/data.avro", object_store=mock_store)
            mock_register.assert_called_once_with("s3://", mock_store, host="bucket")


class TestRegisterArrowObjectStore:
    """Tests for register_arrow with object_store parameter."""

    def test_object_store_triggers_registration(self, ctx):
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            with contextlib.suppress(Exception):
                ctx.register_arrow(
                    "t", "s3://bucket/data.arrow", object_store=mock_store
                )
            mock_register.assert_called_once_with("s3://", mock_store, host="bucket")


class TestReadArrowObjectStore:
    """Tests for read_arrow with object_store parameter."""

    def test_object_store_triggers_registration(self, ctx):
        mock_store = MagicMock()
        with patch.object(ctx, "register_object_store") as mock_register:
            with contextlib.suppress(Exception):
                ctx.read_arrow("s3://bucket/data.arrow", object_store=mock_store)
            mock_register.assert_called_once_with("s3://", mock_store, host="bucket")


class TestEndToEndWithLocalFileSystem:
    """Integration test using LocalFileSystem object store with register_parquet."""

    def test_register_parquet_with_local_object_store(self, ctx, tmp_path):
        """Verify the full flow works with a real object store and local file."""
        import pyarrow.parquet as pq
        from datafusion.object_store import LocalFileSystem

        # Write a test parquet file
        table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        parquet_path = tmp_path / "test.parquet"
        pq.write_table(table, str(parquet_path))

        # Use file:// URL with LocalFileSystem object store
        store = LocalFileSystem()
        file_url = f"file://{tmp_path}/test.parquet"

        ctx.register_parquet("test_tbl", file_url, object_store=store)
        result = ctx.sql("SELECT * FROM test_tbl").collect()

        assert len(result) == 1
        assert result[0].num_rows == 3
        assert result[0].column("x").to_pylist() == [1, 2, 3]

    def test_read_parquet_with_local_object_store(self, ctx, tmp_path):
        """Verify read_parquet works with object_store parameter."""
        import pyarrow.parquet as pq
        from datafusion.object_store import LocalFileSystem

        table = pa.table({"val": [10, 20, 30]})
        parquet_path = tmp_path / "read_test.parquet"
        pq.write_table(table, str(parquet_path))

        store = LocalFileSystem()
        file_url = f"file://{tmp_path}/read_test.parquet"

        df = ctx.read_parquet(file_url, object_store=store)
        result = df.collect()

        assert len(result) == 1
        assert result[0].column("val").to_pylist() == [10, 20, 30]
