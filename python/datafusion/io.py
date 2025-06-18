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

"""IO read functions using global context."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion.context import SessionContext
from datafusion.dataframe import DataFrame

if TYPE_CHECKING:
    import pathlib

    import pyarrow as pa

    from datafusion.expr import Expr


def read_parquet(
    path: str | pathlib.Path,
    table_partition_cols: list[tuple[str, str | pa.DataType]] | None = None,
    parquet_pruning: bool = True,
    file_extension: str = ".parquet",
    skip_metadata: bool = True,
    schema: pa.Schema | None = None,
    file_sort_order: list[list[Expr]] | None = None,
) -> DataFrame:
    """Read a Parquet source into a :py:class:`~datafusion.dataframe.Dataframe`.

    This function will use the global context. Any functions or tables registered
    with another context may not be accessible when used with a DataFrame created
    using this function.

    Args:
        path: Path to the Parquet file.
        table_partition_cols: Partition columns.
        parquet_pruning: Whether the parquet reader should use the predicate
            to prune row groups.
        file_extension: File extension; only files with this extension are
            selected for data input.
        skip_metadata: Whether the parquet reader should skip any metadata
            that may be in the file schema. This can help avoid schema
            conflicts due to metadata.
        schema: An optional schema representing the parquet files. If None,
            the parquet reader will try to infer it based on data in the
            file.
        file_sort_order: Sort order for the file.

    Returns:
        DataFrame representation of the read Parquet files
    """
    if table_partition_cols is None:
        table_partition_cols = []
    return SessionContext.global_ctx().read_parquet(
        str(path),
        table_partition_cols,
        parquet_pruning,
        file_extension,
        skip_metadata,
        schema,
        file_sort_order,
    )


def read_json(
    path: str | pathlib.Path,
    schema: pa.Schema | None = None,
    schema_infer_max_records: int = 1000,
    file_extension: str = ".json",
    table_partition_cols: list[tuple[str, str | pa.DataType]] | None = None,
    file_compression_type: str | None = None,
) -> DataFrame:
    """Read a line-delimited JSON data source.

    This function will use the global context. Any functions or tables registered
    with another context may not be accessible when used with a DataFrame created
    using this function.

    Args:
        path: Path to the JSON file.
        schema: The data source schema.
        schema_infer_max_records: Maximum number of rows to read from JSON
            files for schema inference if needed.
        file_extension: File extension; only files with this extension are
            selected for data input.
        table_partition_cols: Partition columns.
        file_compression_type: File compression type.

    Returns:
        DataFrame representation of the read JSON files.
    """
    if table_partition_cols is None:
        table_partition_cols = []
    return SessionContext.global_ctx().read_json(
        str(path),
        schema,
        schema_infer_max_records,
        file_extension,
        table_partition_cols,
        file_compression_type,
    )


def read_csv(
    path: str | pathlib.Path | list[str] | list[pathlib.Path],
    schema: pa.Schema | None = None,
    has_header: bool = True,
    delimiter: str = ",",
    schema_infer_max_records: int = 1000,
    file_extension: str = ".csv",
    table_partition_cols: list[tuple[str, str | pa.DataType]] | None = None,
    file_compression_type: str | None = None,
) -> DataFrame:
    """Read a CSV data source.

    This function will use the global context. Any functions or tables registered
    with another context may not be accessible when used with a DataFrame created
    using this function.

    Args:
        path: Path to the CSV file
        schema: An optional schema representing the CSV files. If None, the
            CSV reader will try to infer it based on data in file.
        has_header: Whether the CSV file have a header. If schema inference
            is run on a file with no headers, default column names are
            created.
        delimiter: An optional column delimiter.
        schema_infer_max_records: Maximum number of rows to read from CSV
            files for schema inference if needed.
        file_extension:  File extension; only files with this extension are
            selected for data input.
        table_partition_cols:  Partition columns.
        file_compression_type:  File compression type.

    Returns:
        DataFrame representation of the read CSV files
    """
    if table_partition_cols is None:
        table_partition_cols = []

    path = [str(p) for p in path] if isinstance(path, list) else str(path)

    return SessionContext.global_ctx().read_csv(
        path,
        schema,
        has_header,
        delimiter,
        schema_infer_max_records,
        file_extension,
        table_partition_cols,
        file_compression_type,
    )


def read_avro(
    path: str | pathlib.Path,
    schema: pa.Schema | None = None,
    file_partition_cols: list[tuple[str, str | pa.DataType]] | None = None,
    file_extension: str = ".avro",
) -> DataFrame:
    """Create a :py:class:`DataFrame` for reading Avro data source.

    This function will use the global context. Any functions or tables registered
    with another context may not be accessible when used with a DataFrame created
    using this function.

    Args:
        path: Path to the Avro file.
        schema: The data source schema.
        file_partition_cols: Partition columns.
        file_extension: File extension to select.

    Returns:
        DataFrame representation of the read Avro file
    """
    if file_partition_cols is None:
        file_partition_cols = []
    return SessionContext.global_ctx().read_avro(
        str(path), schema, file_partition_cols, file_extension
    )
