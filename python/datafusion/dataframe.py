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
""":py:class:`DataFrame` is one of the core concepts in DataFusion.

See :ref:`user_guide_concepts` in the online documentation for more information.
"""

from __future__ import annotations

import warnings
from collections.abc import AsyncIterator, Iterable, Iterator, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
    overload,
)

try:
    from warnings import deprecated  # Python 3.13+
except ImportError:
    from typing_extensions import deprecated  # Python 3.12

from datafusion._internal import DataFrame as DataFrameInternal
from datafusion._internal import DataFrameWriteOptions as DataFrameWriteOptionsInternal
from datafusion._internal import InsertOp as InsertOpInternal
from datafusion._internal import ParquetColumnOptions as ParquetColumnOptionsInternal
from datafusion._internal import ParquetWriterOptions as ParquetWriterOptionsInternal
from datafusion.expr import (
    Expr,
    SortExpr,
    SortKey,
    ensure_expr,
    ensure_expr_list,
    expr_list_to_raw_expr_list,
    sort_list_to_raw_sort_list,
)
from datafusion.plan import ExecutionPlan, LogicalPlan
from datafusion.record_batch import RecordBatch, RecordBatchStream

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Callable

    import pandas as pd
    import polars as pl
    import pyarrow as pa

    from datafusion.catalog import Table

from enum import Enum


# excerpt from deltalake
# https://github.com/apache/datafusion-python/pull/981#discussion_r1905619163
class Compression(Enum):
    """Enum representing the available compression types for Parquet files."""

    UNCOMPRESSED = "uncompressed"
    SNAPPY = "snappy"
    GZIP = "gzip"
    BROTLI = "brotli"
    LZ4 = "lz4"
    # lzo is not implemented yet
    # https://github.com/apache/arrow-rs/issues/6970
    # LZO = "lzo"  # noqa: ERA001
    ZSTD = "zstd"
    LZ4_RAW = "lz4_raw"

    @classmethod
    def from_str(cls: type[Compression], value: str) -> Compression:
        """Convert a string to a Compression enum value.

        Args:
            value: The string representation of the compression type.

        Returns:
            The Compression enum lowercase value.

        Raises:
            ValueError: If the string does not match any Compression enum value.
        """
        try:
            return cls(value.lower())
        except ValueError as err:
            valid_values = str([item.value for item in Compression])
            error_msg = f"""
                {value} is not a valid Compression.
                Valid values are: {valid_values}
                """
            raise ValueError(error_msg) from err

    def get_default_level(self) -> int | None:
        """Get the default compression level for the compression type.

        Returns:
            The default compression level for the compression type.
        """
        # GZIP, BROTLI default values from deltalake repo
        # https://github.com/apache/datafusion-python/pull/981#discussion_r1905619163
        # ZSTD default value from delta-rs
        # https://github.com/apache/datafusion-python/pull/981#discussion_r1904789223
        if self == Compression.GZIP:
            return 6
        if self == Compression.BROTLI:
            return 1
        if self == Compression.ZSTD:
            return 4
        return None


class ParquetWriterOptions:
    """Advanced parquet writer options.

    Allows settings the writer options that apply to the entire file. Some options can
    also be set on a column by column basis, with the field ``column_specific_options``
    (see ``ParquetColumnOptions``).
    """

    def __init__(
        self,
        data_pagesize_limit: int = 1024 * 1024,
        write_batch_size: int = 1024,
        writer_version: str = "1.0",
        skip_arrow_metadata: bool = False,
        compression: str | None = "zstd(3)",
        compression_level: int | None = None,
        dictionary_enabled: bool | None = True,
        dictionary_page_size_limit: int = 1024 * 1024,
        statistics_enabled: str | None = "page",
        max_row_group_size: int = 1024 * 1024,
        created_by: str = "datafusion-python",
        column_index_truncate_length: int | None = 64,
        statistics_truncate_length: int | None = None,
        data_page_row_count_limit: int = 20_000,
        encoding: str | None = None,
        bloom_filter_on_write: bool = False,
        bloom_filter_fpp: float | None = None,
        bloom_filter_ndv: int | None = None,
        allow_single_file_parallelism: bool = True,
        maximum_parallel_row_group_writers: int = 1,
        maximum_buffered_record_batches_per_stream: int = 2,
        column_specific_options: dict[str, ParquetColumnOptions] | None = None,
    ) -> None:
        """Initialize the ParquetWriterOptions.

        Args:
            data_pagesize_limit: Sets best effort maximum size of data page in bytes.
            write_batch_size: Sets write_batch_size in bytes.
            writer_version: Sets parquet writer version. Valid values are ``1.0`` and
                ``2.0``.
            skip_arrow_metadata: Skip encoding the embedded arrow metadata in the
                KV_meta.
            compression: Compression type to use. Default is ``zstd(3)``.
                Available compression types are

                - ``uncompressed``: No compression.
                - ``snappy``: Snappy compression.
                - ``gzip(n)``: Gzip compression with level n.
                - ``brotli(n)``: Brotli compression with level n.
                - ``lz4``: LZ4 compression.
                - ``lz4_raw``: LZ4_RAW compression.
                - ``zstd(n)``: Zstandard compression with level n.
            compression_level: Compression level to set.
            dictionary_enabled: Sets if dictionary encoding is enabled. If ``None``,
                uses the default parquet writer setting.
            dictionary_page_size_limit: Sets best effort maximum dictionary page size,
                in bytes.
            statistics_enabled: Sets if statistics are enabled for any column Valid
                values are ``none``, ``chunk``, and ``page``. If ``None``, uses the
                default parquet writer setting.
            max_row_group_size: Target maximum number of rows in each row group
                (defaults to 1M rows). Writing larger row groups requires more memory
                to write, but can get better compression and be faster to read.
            created_by: Sets "created by" property.
            column_index_truncate_length: Sets column index truncate length.
            statistics_truncate_length: Sets statistics truncate length. If ``None``,
                uses the default parquet writer setting.
            data_page_row_count_limit: Sets best effort maximum number of rows in a data
                page.
            encoding: Sets default encoding for any column. Valid values are ``plain``,
                ``plain_dictionary``, ``rle``, ``bit_packed``, ``delta_binary_packed``,
                ``delta_length_byte_array``, ``delta_byte_array``, ``rle_dictionary``,
                and ``byte_stream_split``. If ``None``, uses the default parquet writer
                setting.
            bloom_filter_on_write: Write bloom filters for all columns when creating
                parquet files.
            bloom_filter_fpp: Sets bloom filter false positive probability. If ``None``,
                uses the default parquet writer setting
            bloom_filter_ndv: Sets bloom filter number of distinct values. If ``None``,
                uses the default parquet writer setting.
            allow_single_file_parallelism: Controls whether DataFusion will attempt to
                speed up writing parquet files by serializing them in parallel. Each
                column in each row group in each output file are serialized in parallel
                leveraging a maximum possible core count of
                ``n_files * n_row_groups * n_columns``.
            maximum_parallel_row_group_writers: By default parallel parquet writer is
                tuned for minimum memory usage in a streaming execution plan. You may
                see a performance benefit when writing large parquet files by increasing
                ``maximum_parallel_row_group_writers`` and
                ``maximum_buffered_record_batches_per_stream`` if your system has idle
                cores and can tolerate additional memory usage. Boosting these values is
                likely worthwhile when writing out already in-memory data, such as from
                a cached data frame.
            maximum_buffered_record_batches_per_stream: See
                ``maximum_parallel_row_group_writers``.
            column_specific_options: Overrides options for specific columns. If a column
                is not a part of this dictionary, it will use the parameters provided
                here.
        """
        self.data_pagesize_limit = data_pagesize_limit
        self.write_batch_size = write_batch_size
        self.writer_version = writer_version
        self.skip_arrow_metadata = skip_arrow_metadata
        if compression_level is not None:
            self.compression = f"{compression}({compression_level})"
        else:
            self.compression = compression
        self.dictionary_enabled = dictionary_enabled
        self.dictionary_page_size_limit = dictionary_page_size_limit
        self.statistics_enabled = statistics_enabled
        self.max_row_group_size = max_row_group_size
        self.created_by = created_by
        self.column_index_truncate_length = column_index_truncate_length
        self.statistics_truncate_length = statistics_truncate_length
        self.data_page_row_count_limit = data_page_row_count_limit
        self.encoding = encoding
        self.bloom_filter_on_write = bloom_filter_on_write
        self.bloom_filter_fpp = bloom_filter_fpp
        self.bloom_filter_ndv = bloom_filter_ndv
        self.allow_single_file_parallelism = allow_single_file_parallelism
        self.maximum_parallel_row_group_writers = maximum_parallel_row_group_writers
        self.maximum_buffered_record_batches_per_stream = (
            maximum_buffered_record_batches_per_stream
        )
        self.column_specific_options = column_specific_options


class ParquetColumnOptions:
    """Parquet options for individual columns.

    Contains the available options that can be applied for an individual Parquet column,
    replacing the global options in ``ParquetWriterOptions``.
    """

    def __init__(
        self,
        encoding: str | None = None,
        dictionary_enabled: bool | None = None,
        compression: str | None = None,
        statistics_enabled: str | None = None,
        bloom_filter_enabled: bool | None = None,
        bloom_filter_fpp: float | None = None,
        bloom_filter_ndv: int | None = None,
    ) -> None:
        """Initialize the ParquetColumnOptions.

        Args:
            encoding: Sets encoding for the column path. Valid values are: ``plain``,
                ``plain_dictionary``, ``rle``, ``bit_packed``, ``delta_binary_packed``,
                ``delta_length_byte_array``, ``delta_byte_array``, ``rle_dictionary``,
                and ``byte_stream_split``. These values are not case-sensitive. If
                ``None``, uses the default parquet options
            dictionary_enabled: Sets if dictionary encoding is enabled for the column
                path. If `None`, uses the default parquet options
            compression: Sets default parquet compression codec for the column path.
                Valid values are ``uncompressed``, ``snappy``, ``gzip(level)``, ``lzo``,
                ``brotli(level)``, ``lz4``, ``zstd(level)``, and ``lz4_raw``. These
                values are not case-sensitive. If ``None``, uses the default parquet
                options.
            statistics_enabled: Sets if statistics are enabled for the column Valid
                values are: ``none``, ``chunk``, and ``page`` These values are not case
                sensitive. If ``None``, uses the default parquet options.
            bloom_filter_enabled: Sets if bloom filter is enabled for the column path.
                If ``None``, uses the default parquet options.
            bloom_filter_fpp: Sets bloom filter false positive probability for the
                column path. If ``None``, uses the default parquet options.
            bloom_filter_ndv: Sets bloom filter number of distinct values. If ``None``,
                uses the default parquet options.
        """
        self.encoding = encoding
        self.dictionary_enabled = dictionary_enabled
        self.compression = compression
        self.statistics_enabled = statistics_enabled
        self.bloom_filter_enabled = bloom_filter_enabled
        self.bloom_filter_fpp = bloom_filter_fpp
        self.bloom_filter_ndv = bloom_filter_ndv


class DataFrame:
    """Two dimensional table representation of data.

    DataFrame objects are iterable; iterating over a DataFrame yields
    :class:`datafusion.RecordBatch` instances lazily.

    See :ref:`user_guide_concepts` in the online documentation for more information.
    """

    def __init__(self, df: DataFrameInternal) -> None:
        """This constructor is not to be used by the end user.

        See :py:class:`~datafusion.context.SessionContext` for methods to
        create a :py:class:`DataFrame`.
        """
        self.df = df

    def into_view(self, temporary: bool = False) -> Table:
        """Convert ``DataFrame`` into a :class:`~datafusion.Table`.

        Examples:
            >>> from datafusion import SessionContext
            >>> ctx = SessionContext()
            >>> df = ctx.sql("SELECT 1 AS value")
            >>> view = df.into_view()
            >>> ctx.register_table("values_view", view)
            >>> df.collect()  # The DataFrame is still usable
            >>> ctx.sql("SELECT value FROM values_view").collect()
        """
        from datafusion.catalog import Table as _Table

        return _Table(self.df.into_view(temporary))

    def __getitem__(self, key: str | list[str]) -> DataFrame:
        """Return a new :py:class:`DataFrame` with the specified column or columns.

        Args:
            key: Column name or list of column names to select.

        Returns:
            DataFrame with the specified column or columns.
        """
        return DataFrame(self.df.__getitem__(key))

    def __repr__(self) -> str:
        """Return a string representation of the DataFrame.

        Returns:
            String representation of the DataFrame.
        """
        return self.df.__repr__()

    def _repr_html_(self) -> str:
        return self.df._repr_html_()

    @staticmethod
    def default_str_repr(
        batches: list[pa.RecordBatch],
        schema: pa.Schema,
        has_more: bool,
        table_uuid: str | None = None,
    ) -> str:
        """Return the default string representation of a DataFrame.

        This method is used by the default formatter and implemented in Rust for
        performance reasons.
        """
        return DataFrameInternal.default_str_repr(batches, schema, has_more, table_uuid)

    def describe(self) -> DataFrame:
        """Return the statistics for this DataFrame.

        Only summarized numeric datatypes at the moments and returns nulls
        for non-numeric datatypes.

        The output format is modeled after pandas.

        Returns:
            A summary DataFrame containing statistics.
        """
        return DataFrame(self.df.describe())

    def schema(self) -> pa.Schema:
        """Return the :py:class:`pyarrow.Schema` of this DataFrame.

        The output schema contains information on the name, data type, and
        nullability for each column.

        Returns:
            Describing schema of the DataFrame
        """
        return self.df.schema()

    @deprecated(
        "select_columns() is deprecated. Use :py:meth:`~DataFrame.select` instead"
    )
    def select_columns(self, *args: str) -> DataFrame:
        """Filter the DataFrame by columns.

        Returns:
            DataFrame only containing the specified columns.
        """
        return self.select(*args)

    def select_exprs(self, *args: str) -> DataFrame:
        """Project arbitrary list of expression strings into a new DataFrame.

        This method will parse string expressions into logical plan expressions.
        The output DataFrame has one column for each expression.

        Returns:
            DataFrame only containing the specified columns.
        """
        return self.df.select_exprs(*args)

    def select(self, *exprs: Expr | str) -> DataFrame:
        """Project arbitrary expressions into a new :py:class:`DataFrame`.

        Args:
            exprs: Either column names or :py:class:`~datafusion.expr.Expr` to select.

        Returns:
            DataFrame after projection. It has one column for each expression.

        Example usage:

        The following example will return 3 columns from the original dataframe.
        The first two columns will be the original column ``a`` and ``b`` since the
        string "a" is assumed to refer to column selection. Also a duplicate of
        column ``a`` will be returned with the column name ``alternate_a``::

            df = df.select("a", col("b"), col("a").alias("alternate_a"))

        """
        exprs_internal = expr_list_to_raw_expr_list(exprs)
        return DataFrame(self.df.select(*exprs_internal))

    def drop(self, *columns: str) -> DataFrame:
        """Drop arbitrary amount of columns.

        Column names are case-sensitive and do not require double quotes like
        other operations such as `select`. Leading and trailing double quotes
        are allowed and will be automatically stripped if present.

        Args:
            columns: Column names to drop from the dataframe. Both ``column_name``
                    and ``"column_name"`` are accepted.

        Returns:
            DataFrame with those columns removed in the projection.

        Example Usage::

            df.drop('ID_For_Students')      # Works
            df.drop('"ID_For_Students"')    # Also works (quotes stripped)
        """
        normalized_columns = []
        for col in columns:
            if col.startswith('"') and col.endswith('"'):
                normalized_columns.append(col.strip('"'))  # Strip double quotes
            else:
                normalized_columns.append(col)

        return DataFrame(self.df.drop(*normalized_columns))

    def filter(self, *predicates: Expr | str) -> DataFrame:
        """Return a DataFrame for which ``predicate`` evaluates to ``True``.

        Rows for which ``predicate`` evaluates to ``False`` or ``None`` are filtered
        out. If more than one predicate is provided, these predicates will be
        combined as a logical AND. Each ``predicate`` can be an
        :class:`~datafusion.expr.Expr` created using helper functions such as
        :func:`datafusion.col` or :func:`datafusion.lit`, or a SQL expression string
        that will be parsed against the DataFrame schema. If more complex logic is
        required, see the logical operations in :py:mod:`~datafusion.functions`.

        Example::

            from datafusion import col, lit
            df.filter(col("a") > lit(1))
            df.filter("a > 1")

        Args:
            predicates: Predicate expression(s) or SQL strings to filter the DataFrame.

        Returns:
            DataFrame after filtering.
        """
        df = self.df
        for predicate in predicates:
            expr = (
                self.parse_sql_expr(predicate)
                if isinstance(predicate, str)
                else predicate
            )
            df = df.filter(ensure_expr(expr))
        return DataFrame(df)

    def parse_sql_expr(self, expr: str) -> Expr:
        """Creates logical expression from a SQL query text.

        The expression is created and processed against the current schema.

        Example::

            from datafusion import col, lit
            df.parse_sql_expr("a > 1")

            should produce:

            col("a") > lit(1)

        Args:
            expr: Expression string to be converted to datafusion expression

        Returns:
            Logical expression .
        """
        return Expr(self.df.parse_sql_expr(expr))

    def with_column(self, name: str, expr: Expr | str) -> DataFrame:
        """Add an additional column to the DataFrame.

        The ``expr`` must be an :class:`~datafusion.expr.Expr` constructed with
        :func:`datafusion.col` or :func:`datafusion.lit`, or a SQL expression
        string that will be parsed against the DataFrame schema.

        Example::

            from datafusion import col, lit
            df.with_column("b", col("a") + lit(1))

        Args:
            name: Name of the column to add.
            expr: Expression to compute the column.

        Returns:
            DataFrame with the new column.
        """
        expr = self.parse_sql_expr(expr) if isinstance(expr, str) else expr

        return DataFrame(self.df.with_column(name, ensure_expr(expr)))

    def with_columns(
        self, *exprs: Expr | str | Iterable[Expr | str], **named_exprs: Expr | str
    ) -> DataFrame:
        """Add columns to the DataFrame.

        By passing expressions, iterables of expressions, string SQL expressions,
        or named expressions.
        All expressions must be :class:`~datafusion.expr.Expr` objects created via
        :func:`datafusion.col` or :func:`datafusion.lit`, or SQL expression strings.
        To pass named expressions use the form ``name=Expr``.

        Example usage: The following will add 4 columns labeled ``a``, ``b``, ``c``,
        and ``d``::

            from datafusion import col, lit
            df = df.with_columns(
                col("x").alias("a"),
                [lit(1).alias("b"), col("y").alias("c")],
                d=lit(3)
            )

            Equivalent example using just SQL strings:

            df = df.with_columns(
                "x as a",
                ["1 as b", "y as c"],
                d="3"
            )

        Args:
            exprs: Either a single expression, an iterable of expressions to add or
                   SQL expression strings.
            named_exprs: Named expressions in the form of ``name=expr``

        Returns:
            DataFrame with the new columns added.
        """
        expressions = []
        for expr in exprs:
            if isinstance(expr, str):
                expressions.append(self.parse_sql_expr(expr).expr)
            elif isinstance(expr, Iterable) and not isinstance(
                expr, Expr | str | bytes | bytearray
            ):
                expressions.extend(
                    [
                        self.parse_sql_expr(e).expr
                        if isinstance(e, str)
                        else ensure_expr(e)
                        for e in expr
                    ]
                )
            else:
                expressions.append(ensure_expr(expr))

        for alias, expr in named_exprs.items():
            e = self.parse_sql_expr(expr) if isinstance(expr, str) else expr
            ensure_expr(e)
            expressions.append(e.alias(alias).expr)

        return DataFrame(self.df.with_columns(expressions))

    def with_column_renamed(self, old_name: str, new_name: str) -> DataFrame:
        r"""Rename one column by applying a new projection.

        This is a no-op if the column to be renamed does not exist.

        The method supports case sensitive rename with wrapping column name
        into one the following symbols (" or ' or \`).

        Args:
            old_name: Old column name.
            new_name: New column name.

        Returns:
            DataFrame with the column renamed.
        """
        return DataFrame(self.df.with_column_renamed(old_name, new_name))

    def aggregate(
        self,
        group_by: Sequence[Expr | str] | Expr | str,
        aggs: Sequence[Expr] | Expr,
    ) -> DataFrame:
        """Aggregates the rows of the current DataFrame.

        Args:
            group_by: Sequence of expressions or column names to group by.
            aggs: Sequence of expressions to aggregate.

        Returns:
            DataFrame after aggregation.
        """
        group_by_list = (
            list(group_by)
            if isinstance(group_by, Sequence) and not isinstance(group_by, Expr | str)
            else [group_by]
        )
        aggs_list = (
            list(aggs)
            if isinstance(aggs, Sequence) and not isinstance(aggs, Expr)
            else [aggs]
        )

        group_by_exprs = expr_list_to_raw_expr_list(group_by_list)
        aggs_exprs = ensure_expr_list(aggs_list)
        return DataFrame(self.df.aggregate(group_by_exprs, aggs_exprs))

    def sort(self, *exprs: SortKey) -> DataFrame:
        """Sort the DataFrame by the specified sorting expressions or column names.

        Note that any expression can be turned into a sort expression by
        calling its ``sort`` method.

        Args:
            exprs: Sort expressions or column names, applied in order.

        Returns:
            DataFrame after sorting.
        """
        exprs_raw = sort_list_to_raw_sort_list(exprs)
        return DataFrame(self.df.sort(*exprs_raw))

    def cast(self, mapping: dict[str, pa.DataType[Any]]) -> DataFrame:
        """Cast one or more columns to a different data type.

        Args:
            mapping: Mapped with column as key and column dtype as value.

        Returns:
            DataFrame after casting columns
        """
        exprs = [Expr.column(col).cast(dtype) for col, dtype in mapping.items()]
        return self.with_columns(exprs)

    def limit(self, count: int, offset: int = 0) -> DataFrame:
        """Return a new :py:class:`DataFrame` with a limited number of rows.

        Args:
            count: Number of rows to limit the DataFrame to.
            offset: Number of rows to skip.

        Returns:
            DataFrame after limiting.
        """
        return DataFrame(self.df.limit(count, offset))

    def head(self, n: int = 5) -> DataFrame:
        """Return a new :py:class:`DataFrame` with a limited number of rows.

        Args:
            n: Number of rows to take from the head of the DataFrame.

        Returns:
            DataFrame after limiting.
        """
        return DataFrame(self.df.limit(n, 0))

    def tail(self, n: int = 5) -> DataFrame:
        """Return a new :py:class:`DataFrame` with a limited number of rows.

        Be aware this could be potentially expensive since the row size needs to be
        determined of the dataframe. This is done by collecting it.

        Args:
            n: Number of rows to take from the tail of the DataFrame.

        Returns:
            DataFrame after limiting.
        """
        return DataFrame(self.df.limit(n, max(0, self.count() - n)))

    def collect(self) -> list[pa.RecordBatch]:
        """Execute this :py:class:`DataFrame` and collect results into memory.

        Prior to calling ``collect``, modifying a DataFrame simply updates a plan
        (no actual computation is performed). Calling ``collect`` triggers the
        computation.

        Returns:
            List of :py:class:`pyarrow.RecordBatch` collected from the DataFrame.
        """
        return self.df.collect()

    def collect_column(self, column_name: str) -> pa.Array | pa.ChunkedArray:
        """Executes this :py:class:`DataFrame` for a single column."""
        return self.df.collect_column(column_name)

    def cache(self) -> DataFrame:
        """Cache the DataFrame as a memory table.

        Returns:
            Cached DataFrame.
        """
        return DataFrame(self.df.cache())

    def collect_partitioned(self) -> list[list[pa.RecordBatch]]:
        """Execute this DataFrame and collect all partitioned results.

        This operation returns :py:class:`pyarrow.RecordBatch` maintaining the input
        partitioning.

        Returns:
            List of list of :py:class:`RecordBatch` collected from the
                DataFrame.
        """
        return self.df.collect_partitioned()

    def show(self, num: int = 20) -> None:
        """Execute the DataFrame and print the result to the console.

        Args:
            num: Number of lines to show.
        """
        self.df.show(num)

    def distinct(self) -> DataFrame:
        """Return a new :py:class:`DataFrame` with all duplicated rows removed.

        Returns:
            DataFrame after removing duplicates.
        """
        return DataFrame(self.df.distinct())

    @overload
    def join(
        self,
        right: DataFrame,
        on: str | Sequence[str],
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = "inner",
        *,
        left_on: None = None,
        right_on: None = None,
        join_keys: None = None,
        coalesce_duplicate_keys: bool = True,
    ) -> DataFrame: ...

    @overload
    def join(
        self,
        right: DataFrame,
        on: None = None,
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = "inner",
        *,
        left_on: str | Sequence[str],
        right_on: str | Sequence[str],
        join_keys: tuple[list[str], list[str]] | None = None,
        coalesce_duplicate_keys: bool = True,
    ) -> DataFrame: ...

    @overload
    def join(
        self,
        right: DataFrame,
        on: None = None,
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = "inner",
        *,
        join_keys: tuple[list[str], list[str]],
        left_on: None = None,
        right_on: None = None,
        coalesce_duplicate_keys: bool = True,
    ) -> DataFrame: ...

    def join(
        self,
        right: DataFrame,
        on: str | Sequence[str] | tuple[list[str], list[str]] | None = None,
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = "inner",
        *,
        left_on: str | Sequence[str] | None = None,
        right_on: str | Sequence[str] | None = None,
        join_keys: tuple[list[str], list[str]] | None = None,
        coalesce_duplicate_keys: bool = True,
    ) -> DataFrame:
        """Join this :py:class:`DataFrame` with another :py:class:`DataFrame`.

        `on` has to be provided or both `left_on` and `right_on` in conjunction.

        Args:
            right: Other DataFrame to join with.
            on: Column names to join on in both dataframes.
            how: Type of join to perform. Supported types are "inner", "left",
                "right", "full", "semi", "anti".
            left_on: Join column of the left dataframe.
            right_on: Join column of the right dataframe.
            coalesce_duplicate_keys: When True, coalesce the columns
                from the right DataFrame and left DataFrame
                that have identical names in the ``on`` fields.
            join_keys: Tuple of two lists of column names to join on. [Deprecated]

        Returns:
            DataFrame after join.
        """
        if join_keys is not None:
            warnings.warn(
                "`join_keys` is deprecated, use `on` or `left_on` with `right_on`",
                category=DeprecationWarning,
                stacklevel=2,
            )
            left_on = join_keys[0]
            right_on = join_keys[1]

        # This check is to prevent breaking API changes where users prior to
        # DF 43.0.0 would  pass the join_keys as a positional argument instead
        # of a keyword argument.
        if (
            isinstance(on, tuple)
            and len(on) == 2  # noqa: PLR2004
            and isinstance(on[0], list)
            and isinstance(on[1], list)
        ):
            # We know this is safe because we've checked the types
            left_on = on[0]
            right_on = on[1]
            on = None

        if on is not None:
            if left_on is not None or right_on is not None:
                error_msg = "`left_on` or `right_on` should not provided with `on`"
                raise ValueError(error_msg)
            left_on = on
            right_on = on
        elif left_on is not None or right_on is not None:
            if left_on is None or right_on is None:
                error_msg = "`left_on` and `right_on` should both be provided."
                raise ValueError(error_msg)
        else:
            error_msg = "either `on` or `left_on` and `right_on` should be provided."
            raise ValueError(error_msg)
        if isinstance(left_on, str):
            left_on = [left_on]
        if isinstance(right_on, str):
            right_on = [right_on]

        return DataFrame(
            self.df.join(right.df, how, left_on, right_on, coalesce_duplicate_keys)
        )

    def join_on(
        self,
        right: DataFrame,
        *on_exprs: Expr,
        how: Literal["inner", "left", "right", "full", "semi", "anti"] = "inner",
    ) -> DataFrame:
        """Join two :py:class:`DataFrame` using the specified expressions.

        Join predicates must be :class:`~datafusion.expr.Expr` objects, typically
        built with :func:`datafusion.col`. On expressions are used to support
        in-equality predicates. Equality predicates are correctly optimized.

        Example::

            from datafusion import col
            df.join_on(other_df, col("id") == col("other_id"))

        Args:
            right: Other DataFrame to join with.
            on_exprs: single or multiple (in)-equality predicates.
            how: Type of join to perform. Supported types are "inner", "left",
                "right", "full", "semi", "anti".

        Returns:
            DataFrame after join.
        """
        exprs = [ensure_expr(expr) for expr in on_exprs]
        return DataFrame(self.df.join_on(right.df, exprs, how))

    def explain(self, verbose: bool = False, analyze: bool = False) -> None:
        """Print an explanation of the DataFrame's plan so far.

        If ``analyze`` is specified, runs the plan and reports metrics.

        Args:
            verbose: If ``True``, more details will be included.
            analyze: If ``True``, the plan will run and metrics reported.
        """
        self.df.explain(verbose, analyze)

    def logical_plan(self) -> LogicalPlan:
        """Return the unoptimized ``LogicalPlan``.

        Returns:
            Unoptimized logical plan.
        """
        return LogicalPlan(self.df.logical_plan())

    def optimized_logical_plan(self) -> LogicalPlan:
        """Return the optimized ``LogicalPlan``.

        Returns:
            Optimized logical plan.
        """
        return LogicalPlan(self.df.optimized_logical_plan())

    def execution_plan(self) -> ExecutionPlan:
        """Return the execution/physical plan.

        Returns:
            Execution plan.
        """
        return ExecutionPlan(self.df.execution_plan())

    def repartition(self, num: int) -> DataFrame:
        """Repartition a DataFrame into ``num`` partitions.

        The batches allocation uses a round-robin algorithm.

        Args:
            num: Number of partitions to repartition the DataFrame into.

        Returns:
            Repartitioned DataFrame.
        """
        return DataFrame(self.df.repartition(num))

    def repartition_by_hash(self, *exprs: Expr | str, num: int) -> DataFrame:
        """Repartition a DataFrame using a hash partitioning scheme.

        Args:
            exprs: Expressions or a SQL expression string to evaluate
                   and perform hashing on.
            num: Number of partitions to repartition the DataFrame into.

        Returns:
            Repartitioned DataFrame.
        """
        exprs = [self.parse_sql_expr(e) if isinstance(e, str) else e for e in exprs]
        exprs = expr_list_to_raw_expr_list(exprs)

        return DataFrame(self.df.repartition_by_hash(*exprs, num=num))

    def union(self, other: DataFrame, distinct: bool = False) -> DataFrame:
        """Calculate the union of two :py:class:`DataFrame`.

        The two :py:class:`DataFrame` must have exactly the same schema.

        Args:
            other: DataFrame to union with.
            distinct: If ``True``, duplicate rows will be removed.

        Returns:
            DataFrame after union.
        """
        return DataFrame(self.df.union(other.df, distinct))

    def union_distinct(self, other: DataFrame) -> DataFrame:
        """Calculate the distinct union of two :py:class:`DataFrame`.

        The two :py:class:`DataFrame` must have exactly the same schema.
        Any duplicate rows are discarded.

        Args:
            other: DataFrame to union with.

        Returns:
            DataFrame after union.
        """
        return DataFrame(self.df.union_distinct(other.df))

    def intersect(self, other: DataFrame) -> DataFrame:
        """Calculate the intersection of two :py:class:`DataFrame`.

        The two :py:class:`DataFrame` must have exactly the same schema.

        Args:
            other:  DataFrame to intersect with.

        Returns:
            DataFrame after intersection.
        """
        return DataFrame(self.df.intersect(other.df))

    def except_all(self, other: DataFrame) -> DataFrame:
        """Calculate the exception of two :py:class:`DataFrame`.

        The two :py:class:`DataFrame` must have exactly the same schema.

        Args:
            other: DataFrame to calculate exception with.

        Returns:
            DataFrame after exception.
        """
        return DataFrame(self.df.except_all(other.df))

    def write_csv(
        self,
        path: str | pathlib.Path,
        with_header: bool = False,
        write_options: DataFrameWriteOptions | None = None,
    ) -> None:
        """Execute the :py:class:`DataFrame`  and write the results to a CSV file.

        Args:
            path: Path of the CSV file to write.
            with_header: If true, output the CSV header row.
            write_options: Options that impact how the DataFrame is written.
        """
        raw_write_options = (
            write_options._raw_write_options if write_options is not None else None
        )
        self.df.write_csv(str(path), with_header, raw_write_options)

    @overload
    def write_parquet(
        self,
        path: str | pathlib.Path,
        compression: str,
        compression_level: int | None = None,
        write_options: DataFrameWriteOptions | None = None,
    ) -> None: ...

    @overload
    def write_parquet(
        self,
        path: str | pathlib.Path,
        compression: Compression = Compression.ZSTD,
        compression_level: int | None = None,
        write_options: DataFrameWriteOptions | None = None,
    ) -> None: ...

    @overload
    def write_parquet(
        self,
        path: str | pathlib.Path,
        compression: ParquetWriterOptions,
        compression_level: None = None,
        write_options: DataFrameWriteOptions | None = None,
    ) -> None: ...

    def write_parquet(
        self,
        path: str | pathlib.Path,
        compression: str | Compression | ParquetWriterOptions = Compression.ZSTD,
        compression_level: int | None = None,
        write_options: DataFrameWriteOptions | None = None,
    ) -> None:
        """Execute the :py:class:`DataFrame` and write the results to a Parquet file.

        Available compression types are:

        - "uncompressed": No compression.
        - "snappy": Snappy compression.
        - "gzip": Gzip compression.
        - "brotli": Brotli compression.
        - "lz4": LZ4 compression.
        - "lz4_raw": LZ4_RAW compression.
        - "zstd": Zstandard compression.

        LZO compression is not yet implemented in arrow-rs and is therefore
        excluded.

        Args:
            path: Path of the Parquet file to write.
            compression: Compression type to use. Default is "ZSTD".
            compression_level: Compression level to use. For ZSTD, the
                recommended range is 1 to 22, with the default being 4. Higher levels
                provide better compression but slower speed.
            write_options: Options that impact how the DataFrame is written.
        """
        if isinstance(compression, ParquetWriterOptions):
            if compression_level is not None:
                msg = "compression_level should be None when using ParquetWriterOptions"
                raise ValueError(msg)
            self.write_parquet_with_options(path, compression)
            return

        if isinstance(compression, str):
            compression = Compression.from_str(compression)

        if (
            compression in {Compression.GZIP, Compression.BROTLI, Compression.ZSTD}
            and compression_level is None
        ):
            compression_level = compression.get_default_level()

        raw_write_options = (
            write_options._raw_write_options if write_options is not None else None
        )
        self.df.write_parquet(
            str(path),
            compression.value,
            compression_level,
            raw_write_options,
        )

    def write_parquet_with_options(
        self,
        path: str | pathlib.Path,
        options: ParquetWriterOptions,
        write_options: DataFrameWriteOptions | None = None,
    ) -> None:
        """Execute the :py:class:`DataFrame` and write the results to a Parquet file.

        Allows advanced writer options to be set with `ParquetWriterOptions`.

        Args:
            path: Path of the Parquet file to write.
            options: Sets the writer parquet options (see `ParquetWriterOptions`).
            write_options: Options that impact how the DataFrame is written.
        """
        options_internal = ParquetWriterOptionsInternal(
            options.data_pagesize_limit,
            options.write_batch_size,
            options.writer_version,
            options.skip_arrow_metadata,
            options.compression,
            options.dictionary_enabled,
            options.dictionary_page_size_limit,
            options.statistics_enabled,
            options.max_row_group_size,
            options.created_by,
            options.column_index_truncate_length,
            options.statistics_truncate_length,
            options.data_page_row_count_limit,
            options.encoding,
            options.bloom_filter_on_write,
            options.bloom_filter_fpp,
            options.bloom_filter_ndv,
            options.allow_single_file_parallelism,
            options.maximum_parallel_row_group_writers,
            options.maximum_buffered_record_batches_per_stream,
        )

        column_specific_options_internal = {}
        for column, opts in (options.column_specific_options or {}).items():
            column_specific_options_internal[column] = ParquetColumnOptionsInternal(
                bloom_filter_enabled=opts.bloom_filter_enabled,
                encoding=opts.encoding,
                dictionary_enabled=opts.dictionary_enabled,
                compression=opts.compression,
                statistics_enabled=opts.statistics_enabled,
                bloom_filter_fpp=opts.bloom_filter_fpp,
                bloom_filter_ndv=opts.bloom_filter_ndv,
            )

        raw_write_options = (
            write_options._raw_write_options if write_options is not None else None
        )
        self.df.write_parquet_with_options(
            str(path),
            options_internal,
            column_specific_options_internal,
            raw_write_options,
        )

    def write_json(
        self,
        path: str | pathlib.Path,
        write_options: DataFrameWriteOptions | None = None,
    ) -> None:
        """Execute the :py:class:`DataFrame` and write the results to a JSON file.

        Args:
            path: Path of the JSON file to write.
            write_options: Options that impact how the DataFrame is written.
        """
        raw_write_options = (
            write_options._raw_write_options if write_options is not None else None
        )
        self.df.write_json(str(path), write_options=raw_write_options)

    def write_table(
        self, table_name: str, write_options: DataFrameWriteOptions | None = None
    ) -> None:
        """Execute the :py:class:`DataFrame` and write the results to a table.

        The table must be registered with the session to perform this operation.
        Not all table providers support writing operations. See the individual
        implementations for details.
        """
        raw_write_options = (
            write_options._raw_write_options if write_options is not None else None
        )
        self.df.write_table(table_name, raw_write_options)

    def to_arrow_table(self) -> pa.Table:
        """Execute the :py:class:`DataFrame` and convert it into an Arrow Table.

        Returns:
            Arrow Table.
        """
        return self.df.to_arrow_table()

    def execute_stream(self) -> RecordBatchStream:
        """Executes this DataFrame and returns a stream over a single partition.

        Returns:
            Record Batch Stream over a single partition.
        """
        return RecordBatchStream(self.df.execute_stream())

    def execute_stream_partitioned(self) -> list[RecordBatchStream]:
        """Executes this DataFrame and returns a stream for each partition.

        Returns:
            One record batch stream per partition.
        """
        streams = self.df.execute_stream_partitioned()
        return [RecordBatchStream(rbs) for rbs in streams]

    def to_pandas(self) -> pd.DataFrame:
        """Execute the :py:class:`DataFrame` and convert it into a Pandas DataFrame.

        Returns:
            Pandas DataFrame.
        """
        return self.df.to_pandas()

    def to_pylist(self) -> list[dict[str, Any]]:
        """Execute the :py:class:`DataFrame` and convert it into a list of dictionaries.

        Returns:
            List of dictionaries.
        """
        return self.df.to_pylist()

    def to_pydict(self) -> dict[str, list[Any]]:
        """Execute the :py:class:`DataFrame` and convert it into a dictionary of lists.

        Returns:
            Dictionary of lists.
        """
        return self.df.to_pydict()

    def to_polars(self) -> pl.DataFrame:
        """Execute the :py:class:`DataFrame` and convert it into a Polars DataFrame.

        Returns:
            Polars DataFrame.
        """
        return self.df.to_polars()

    def count(self) -> int:
        """Return the total number of rows in this :py:class:`DataFrame`.

        Note that this method will actually run a plan to calculate the
        count, which may be slow for large or complicated DataFrames.

        Returns:
            Number of rows in the DataFrame.
        """
        return self.df.count()

    @deprecated("Use :py:func:`unnest_columns` instead.")
    def unnest_column(self, column: str, preserve_nulls: bool = True) -> DataFrame:
        """See :py:func:`unnest_columns`."""
        return DataFrame(self.df.unnest_column(column, preserve_nulls=preserve_nulls))

    def unnest_columns(self, *columns: str, preserve_nulls: bool = True) -> DataFrame:
        """Expand columns of arrays into a single row per array element.

        Args:
            columns: Column names to perform unnest operation on.
            preserve_nulls: If False, rows with null entries will not be
                returned.

        Returns:
            A DataFrame with the columns expanded.
        """
        columns = list(columns)
        return DataFrame(self.df.unnest_columns(columns, preserve_nulls=preserve_nulls))

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """Export the DataFrame as an Arrow C Stream.

        The DataFrame is executed using DataFusion's streaming APIs and exposed via
        Arrow's C Stream interface. Record batches are produced incrementally, so the
        full result set is never materialized in memory.

        When ``requested_schema`` is provided, DataFusion applies only simple
        projections such as selecting a subset of existing columns or reordering
        them. Column renaming, computed expressions, or type coercion are not
        supported through this interface.

        Args:
            requested_schema: Either a :py:class:`pyarrow.Schema` or an Arrow C
                Schema capsule (``PyCapsule``) produced by
                ``schema._export_to_c_capsule()``. The DataFrame will attempt to
                align its output with the fields and order specified by this schema.

        Returns:
            Arrow ``PyCapsule`` object representing an ``ArrowArrayStream``.

        For practical usage patterns, see the Apache Arrow streaming
        documentation: https://arrow.apache.org/docs/python/ipc.html#streaming.

        For details on DataFusion's Arrow integration and DataFrame streaming,
        see the user guide (user-guide/io/arrow and user-guide/dataframe/index).

        Notes:
            The Arrow C Data Interface PyCapsule details are documented by Apache
            Arrow and can be found at:
            https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
        """
        # ``DataFrame.__arrow_c_stream__`` in the Rust extension leverages
        # ``execute_stream_partitioned`` under the hood to stream batches while
        # preserving the original partition order.
        return self.df.__arrow_c_stream__(requested_schema)

    def __iter__(self) -> Iterator[RecordBatch]:
        """Return an iterator over this DataFrame's record batches."""
        return iter(self.execute_stream())

    def __aiter__(self) -> AsyncIterator[RecordBatch]:
        """Return an async iterator over this DataFrame's record batches.

        We're using __aiter__ because we support Python < 3.10 where aiter() is not
        available.
        """
        return self.execute_stream().__aiter__()

    def transform(self, func: Callable[..., DataFrame], *args: Any) -> DataFrame:
        """Apply a function to the current DataFrame which returns another DataFrame.

        This is useful for chaining together multiple functions. For example::

            def add_3(df: DataFrame) -> DataFrame:
                return df.with_column("modified", lit(3))

            def within_limit(df: DataFrame, limit: int) -> DataFrame:
                return df.filter(col("a") < lit(limit)).distinct()

            df = df.transform(modify_df).transform(within_limit, 4)

        Args:
            func: A callable function that takes a DataFrame as it's first argument
            args: Zero or more arguments to pass to `func`

        Returns:
            DataFrame: After applying func to the original dataframe.
        """
        return func(self, *args)

    def fill_null(self, value: Any, subset: list[str] | None = None) -> DataFrame:
        """Fill null values in specified columns with a value.

        Args:
            value: Value to replace nulls with. Will be cast to match column type.
            subset: Optional list of column names to fill. If None, fills all columns.

        Returns:
            DataFrame with null values replaced where type casting is possible

        Examples:
            >>> df = df.fill_null(0)  # Fill all nulls with 0 where possible
            >>> # Fill nulls in specific string columns
            >>> df = df.fill_null("missing", subset=["name", "category"])

        Notes:
            - Only fills nulls in columns where the value can be cast to the column type
            - For columns where casting fails, the original column is kept unchanged
            - For columns not in subset, the original column is kept unchanged
        """
        return DataFrame(self.df.fill_null(value, subset))


class InsertOp(Enum):
    """Insert operation mode.

    These modes are used by the table writing feature to define how record
    batches should be written to a table.
    """

    APPEND = InsertOpInternal.APPEND
    """Appends new rows to the existing table without modifying any existing rows."""

    REPLACE = InsertOpInternal.REPLACE
    """Replace existing rows that collide with the inserted rows.

    Replacement is typically based on a unique key or primary key.
    """

    OVERWRITE = InsertOpInternal.OVERWRITE
    """Overwrites all existing rows in the table with the new rows."""


class DataFrameWriteOptions:
    """Writer options for DataFrame.

    There is no guarantee the table provider supports all writer options.
    See the individual implementation and documentation for details.
    """

    def __init__(
        self,
        insert_operation: InsertOp | None = None,
        single_file_output: bool = False,
        partition_by: str | Sequence[str] | None = None,
        sort_by: Expr | SortExpr | Sequence[Expr] | Sequence[SortExpr] | None = None,
    ) -> None:
        """Instantiate writer options for DataFrame."""
        if isinstance(partition_by, str):
            partition_by = [partition_by]

        sort_by_raw = sort_list_to_raw_sort_list(sort_by)
        insert_op = insert_operation.value if insert_operation is not None else None

        self._raw_write_options = DataFrameWriteOptionsInternal(
            insert_op, single_file_output, partition_by, sort_by_raw
        )
