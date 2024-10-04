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

from typing import Any, List, TYPE_CHECKING
from datafusion.record_batch import RecordBatchStream
from typing_extensions import deprecated
from datafusion.plan import LogicalPlan, ExecutionPlan

if TYPE_CHECKING:
    import pyarrow as pa
    import pandas as pd
    import polars as pl
    import pathlib
    from typing import Callable

from datafusion._internal import DataFrame as DataFrameInternal
from datafusion.expr import Expr, SortExpr, sort_or_default


class DataFrame:
    """Two dimensional table representation of data.

    See :ref:`user_guide_concepts` in the online documentation for more information.
    """

    def __init__(self, df: DataFrameInternal) -> None:
        """This constructor is not to be used by the end user.

        See :py:class:`~datafusion.context.SessionContext` for methods to
        create a :py:class:`DataFrame`.
        """
        self.df = df

    def __getitem__(self, key: str | List[str]) -> DataFrame:
        """Return a new :py:class`DataFrame` with the specified column or columns.

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

    def select_columns(self, *args: str) -> DataFrame:
        """Filter the DataFrame by columns.

        Returns:
            DataFrame only containing the specified columns.
        """
        return self.select(*args)

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
        exprs_internal = [
            Expr.column(arg).expr if isinstance(arg, str) else arg.expr for arg in exprs
        ]
        return DataFrame(self.df.select(*exprs_internal))

    def filter(self, *predicates: Expr) -> DataFrame:
        """Return a DataFrame for which ``predicate`` evaluates to ``True``.

        Rows for which ``predicate`` evaluates to ``False`` or ``None`` are filtered
        out.  If more than one predicate is provided, these predicates will be
        combined as a logical AND. If more complex logic is required, see the
        logical operations in :py:mod:`~datafusion.functions`.

        Args:
            predicates: Predicate expression(s) to filter the DataFrame.

        Returns:
            DataFrame after filtering.
        """
        df = self.df
        for p in predicates:
            df = df.filter(p.expr)
        return DataFrame(df)

    def with_column(self, name: str, expr: Expr) -> DataFrame:
        """Add an additional column to the DataFrame.

        Args:
            name: Name of the column to add.
            expr: Expression to compute the column.

        Returns:
            DataFrame with the new column.
        """
        return DataFrame(self.df.with_column(name, expr.expr))

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
        self, group_by: list[Expr] | Expr, aggs: list[Expr] | Expr
    ) -> DataFrame:
        """Aggregates the rows of the current DataFrame.

        Args:
            group_by: List of expressions to group by.
            aggs: List of expressions to aggregate.

        Returns:
            DataFrame after aggregation.
        """
        group_by = group_by if isinstance(group_by, list) else [group_by]
        aggs = aggs if isinstance(aggs, list) else [aggs]

        group_by = [e.expr for e in group_by]
        aggs = [e.expr for e in aggs]
        return DataFrame(self.df.aggregate(group_by, aggs))

    def sort(self, *exprs: Expr | SortExpr) -> DataFrame:
        """Sort the DataFrame by the specified sorting expressions.

        Note that any expression can be turned into a sort expression by
        calling its` ``sort`` method.

        Args:
            exprs: Sort expressions, applied in order.

        Returns:
            DataFrame after sorting.
        """
        exprs_raw = [sort_or_default(expr) for expr in exprs]
        return DataFrame(self.df.sort(*exprs_raw))

    def limit(self, count: int, offset: int = 0) -> DataFrame:
        """Return a new :py:class:`DataFrame` with a limited number of rows.

        Args:
            count: Number of rows to limit the DataFrame to.
            offset: Number of rows to skip.

        Returns:
            DataFrame after limiting.
        """
        return DataFrame(self.df.limit(count, offset))

    def collect(self) -> list[pa.RecordBatch]:
        """Execute this :py:class:`DataFrame` and collect results into memory.

        Prior to calling ``collect``, modifying a DataFrme simply updates a plan
        (no actual computation is performed). Calling ``collect`` triggers the
        computation.

        Returns:
            List of :py:class:`pyarrow.RecordBatch` collected from the DataFrame.
        """
        return self.df.collect()

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

    def join(
        self,
        right: DataFrame,
        join_keys: tuple[list[str], list[str]],
        how: str,
    ) -> DataFrame:
        """Join this :py:class:`DataFrame` with another :py:class:`DataFrame`.

        Join keys are a pair of lists of column names in the left and right
        dataframes, respectively. These lists must have the same length.

        Args:
            right: Other DataFrame to join with.
            join_keys: Tuple of two lists of column names to join on.
            how: Type of join to perform. Supported types are "inner", "left",
                "right", "full", "semi", "anti".

        Returns:
            DataFrame after join.
        """
        return DataFrame(self.df.join(right.df, join_keys, how))

    def explain(self, verbose: bool = False, analyze: bool = False) -> DataFrame:
        """Return a DataFrame with the explanation of its plan so far.

        If ``analyze`` is specified, runs the plan and reports metrics.

        Args:
            verbose: If ``True``, more details will be included.
            analyze: If ``Tru`e``, the plan will run and metrics reported.

        Returns:
            DataFrame with the explanation of its plan.
        """
        return DataFrame(self.df.explain(verbose, analyze))

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

    def repartition_by_hash(self, *exprs: Expr, num: int) -> DataFrame:
        """Repartition a DataFrame using a hash partitioning scheme.

        Args:
            exprs: Expressions to evaluate and perform hashing on.
            num: Number of partitions to repartition the DataFrame into.

        Returns:
            Repartitioned DataFrame.
        """
        exprs = [expr.expr for expr in exprs]
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

    def write_csv(self, path: str | pathlib.Path, with_header: bool = False) -> None:
        """Execute the :py:class:`DataFrame`  and write the results to a CSV file.

        Args:
            path: Path of the CSV file to write.
            with_header: If true, output the CSV header row.
        """
        self.df.write_csv(str(path), with_header)

    def write_parquet(
        self,
        path: str | pathlib.Path,
        compression: str = "uncompressed",
        compression_level: int | None = None,
    ) -> None:
        """Execute the :py:class:`DataFrame` and write the results to a Parquet file.

        Args:
            path: Path of the Parquet file to write.
            compression: Compression type to use.
            compression_level: Compression level to use.
        """
        self.df.write_parquet(str(path), compression, compression_level)

    def write_json(self, path: str | pathlib.Path) -> None:
        """Execute the :py:class:`DataFrame` and write the results to a JSON file.

        Args:
            path: Path of the JSON file to write.
        """
        self.df.write_json(str(path))

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
        columns = [c for c in columns]
        return DataFrame(self.df.unnest_columns(columns, preserve_nulls=preserve_nulls))

    def __arrow_c_stream__(self, requested_schema: pa.Schema) -> Any:
        """Export an Arrow PyCapsule Stream.

        This will execute and collect the DataFrame. We will attempt to respect the
        requested schema, but only trivial transformations will be applied such as only
        returning the fields listed in the requested schema if their data types match
        those in the DataFrame.

        Args:
            requested_schema: Attempt to provide the DataFrame using this schema.

        Returns:
            Arrow PyCapsule object.
        """
        return self.df.__arrow_c_stream__(requested_schema)

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
