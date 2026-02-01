# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Options for reading various file formats."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from datafusion.expr import SortExpr

from ._internal import options

__all__ = ["CsvReadOptions"]

DEFAULT_MAX_INFER_SCHEMA = 1000


class CsvReadOptions:
    """Options for reading CSV files.

    This class provides a builder pattern for configuring CSV reading options.
    All methods starting with ``with_`` return ``self`` to allow method chaining.
    """

    def __init__(
        self,
        *,
        has_header: bool = True,
        delimiter: str = ",",
        quote: str = '"',
        terminator: str | None = None,
        escape: str | None = None,
        comment: str | None = None,
        newlines_in_values: bool = False,
        schema: pa.Schema | None = None,
        schema_infer_max_records: int = DEFAULT_MAX_INFER_SCHEMA,
        file_extension: str = ".csv",
        table_partition_cols: list[tuple[str, pa.DataType]] | None = None,
        file_compression_type: str = "",
        file_sort_order: list[list[SortExpr]] | None = None,
        null_regex: str | None = None,
        truncated_rows: bool = False,
    ) -> None:
        """Initialize CsvReadOptions.

        Args:
            has_header: Does the CSV file have a header row? If schema inference
                is run on a file with no headers, default column names are created.
            delimiter: Column delimiter character. Must be a single ASCII character.
            quote: Quote character for fields containing delimiters or newlines.
                Must be a single ASCII character.
            terminator: Optional line terminator character. If ``None``, uses CRLF.
                Must be a single ASCII character.
            escape: Optional escape character for quotes. Must be a single ASCII
                character.
            comment: If specified, lines beginning with this character are ignored.
                Must be a single ASCII character.
            newlines_in_values: Whether newlines in quoted values are supported.
                Parsing newlines in quoted values may be affected by execution
                behavior such as parallel file scanning. Setting this to ``True``
                ensures that newlines in values are parsed successfully, which may
                reduce performance.
            schema: Optional PyArrow schema representing the CSV files. If ``None``,
                the CSV reader will try to infer it based on data in the file.
            schema_infer_max_records: Maximum number of rows to read from CSV files
                for schema inference if needed.
            file_extension: File extension; only files with this extension are
                selected for data input.
            table_partition_cols: Partition columns as a list of tuples of
                (column_name, data_type).
            file_compression_type: File compression type. Supported values are
                ``"gzip"``, ``"bz2"``, ``"xz"``, ``"zstd"``, or empty string for
                uncompressed.
            file_sort_order: Optional sort order of the files as a list of sort
                expressions per file.
            null_regex: Optional regex pattern to match null values in the CSV.
            truncated_rows: Whether to allow truncated rows when parsing. By default
                this is ``False`` and will error if the CSV rows have different
                lengths. When set to ``True``, it will allow records with less than
                the expected number of columns and fill the missing columns with
                nulls. If the record's schema is not nullable, it will still return
                an error.
        """
        validate_single_character("delimiter", delimiter)
        validate_single_character("quote", quote)
        validate_single_character("terminator", terminator)
        validate_single_character("escape", escape)
        validate_single_character("comment", comment)

        self.has_header = has_header
        self.delimiter = delimiter
        self.quote = quote
        self.terminator = terminator
        self.escape = escape
        self.comment = comment
        self.newlines_in_values = newlines_in_values
        self.schema = schema
        self.schema_infer_max_records = schema_infer_max_records
        self.file_extension = file_extension
        self.table_partition_cols = table_partition_cols or []
        self.file_compression_type = file_compression_type
        self.file_sort_order = file_sort_order or []
        self.null_regex = null_regex
        self.truncated_rows = truncated_rows

    def with_has_header(self, has_header: bool) -> CsvReadOptions:
        """Configure whether the CSV has a header row."""
        self.has_header = has_header
        return self

    def with_delimiter(self, delimiter: str) -> CsvReadOptions:
        """Configure the column delimiter."""
        self.delimiter = delimiter
        return self

    def with_quote(self, quote: str) -> CsvReadOptions:
        """Configure the quote character."""
        self.quote = quote
        return self

    def with_terminator(self, terminator: str | None) -> CsvReadOptions:
        """Configure the line terminator character."""
        self.terminator = terminator
        return self

    def with_escape(self, escape: str | None) -> CsvReadOptions:
        """Configure the escape character."""
        self.escape = escape
        return self

    def with_comment(self, comment: str | None) -> CsvReadOptions:
        """Configure the comment character."""
        self.comment = comment
        return self

    def with_newlines_in_values(self, newlines_in_values: bool) -> CsvReadOptions:
        """Configure whether newlines in values are supported."""
        self.newlines_in_values = newlines_in_values
        return self

    def with_schema(self, schema: pa.Schema | None) -> CsvReadOptions:
        """Configure the schema."""
        self.schema = schema
        return self

    def with_schema_infer_max_records(
        self, schema_infer_max_records: int
    ) -> CsvReadOptions:
        """Configure maximum records for schema inference."""
        self.schema_infer_max_records = schema_infer_max_records
        return self

    def with_file_extension(self, file_extension: str) -> CsvReadOptions:
        """Configure the file extension filter."""
        self.file_extension = file_extension
        return self

    def with_table_partition_cols(
        self, table_partition_cols: list[tuple[str, pa.DataType]]
    ) -> CsvReadOptions:
        """Configure table partition columns."""
        self.table_partition_cols = table_partition_cols
        return self

    def with_file_compression_type(self, file_compression_type: str) -> CsvReadOptions:
        """Configure file compression type."""
        self.file_compression_type = file_compression_type
        return self

    def with_file_sort_order(
        self, file_sort_order: list[list[SortExpr]]
    ) -> CsvReadOptions:
        """Configure file sort order."""
        self.file_sort_order = file_sort_order
        return self

    def with_null_regex(self, null_regex: str | None) -> CsvReadOptions:
        """Configure null value regex pattern."""
        self.null_regex = null_regex
        return self

    def with_truncated_rows(self, truncated_rows: bool) -> CsvReadOptions:
        """Configure whether to allow truncated rows."""
        self.truncated_rows = truncated_rows
        return self

    def to_inner(self) -> options.CsvReadOptions:
        """Convert this object into the underlying Rust structure.

        This is intended for internal use only.
        """
        return options.CsvReadOptions(
            has_header=self.has_header,
            delimiter=ord(self.delimiter[0]) if self.delimiter else ord(","),
            quote=ord(self.quote[0]) if self.quote else ord('"'),
            terminator=ord(self.terminator[0]) if self.terminator else None,
            escape=ord(self.escape[0]) if self.escape else None,
            comment=ord(self.comment[0]) if self.comment else None,
            newlines_in_values=self.newlines_in_values,
            schema=self.schema,
            schema_infer_max_records=self.schema_infer_max_records,
            file_extension=self.file_extension,
            table_partition_cols=_convert_table_partition_cols(
                self.table_partition_cols
            ),
            file_compression_type=self.file_compression_type or "",
            file_sort_order=self.file_sort_order or [],
            null_regex=self.null_regex,
            truncated_rows=self.truncated_rows,
        )


def validate_single_character(name: str, value: str | None) -> None:
    if value is not None and len(value) != 1:
        message = f"{name} must be a single character"
        raise ValueError(message)


def _convert_table_partition_cols(
    table_partition_cols: list[tuple[str, str | pa.DataType]],
) -> list[tuple[str, pa.DataType]]:
    warn = False
    converted_table_partition_cols = []

    for col, data_type in table_partition_cols:
        if isinstance(data_type, str):
            warn = True
            if data_type == "string":
                converted_data_type = pa.string()
            elif data_type == "int":
                converted_data_type = pa.int32()
            else:
                message = (
                    f"Unsupported literal data type '{data_type}' for partition "
                    "column. Supported types are 'string' and 'int'"
                )
                raise ValueError(message)
        else:
            converted_data_type = data_type

        converted_table_partition_cols.append((col, converted_data_type))

    if warn:
        message = (
            "using literals for table_partition_cols data types is deprecated,"
            "use pyarrow types instead"
        )
        warnings.warn(
            message,
            category=DeprecationWarning,
            stacklevel=2,
        )

    return converted_table_partition_cols
