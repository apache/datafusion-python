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
"""DataFrame is one of the core concepts in DataFusion.

See https://datafusion.apache.org/python/user-guide/basics.html for more information.
"""

from __future__ import annotations

from typing import Any, List, TYPE_CHECKING
from datafusion.record_batch import RecordBatchStream
from typing_extensions import deprecated

if TYPE_CHECKING:
    import pyarrow as pa
    import pandas as pd
    import polars as pl
    import pathlib

from datafusion._internal import DataFrame as DataFrameInternal
from datafusion.expr import Expr
from datafusion._internal import (
    LogicalPlan,
    ExecutionPlan,
)  # TODO make these first class python classes


class DataFrame:
    """Two dimensional representation of data represented as rows and columns in a table.

    See https://datafusion.apache.org/python/user-guide/basics.html for more information.
    """

    def __init__(self, df: DataFrameInternal) -> None:
        """This constructor is not to be used by the end user.

        See ``SessionContext`` for methods to create DataFrames.
        """
        self.df = df

    def __getitem__(self, key: str | List[str]) -> DataFrame:
        """Return a new `DataFrame` with the specified column or columns.

        Parameters
        ----------
        key : Any
            Column name or list of column names to select.

        Returns:
        -------
        DataFrame
            DataFrame with the specified column or columns.
        """
        return DataFrame(self.df.__getitem__(key))

    def __repr__(self) -> str:
        """Return a string representation of the DataFrame.

        Returns:
        -------
        str
            String representation of the DataFrame.
        """
        return self.df.__repr__()

    def describe(self) -> DataFrame:
        """Return a new `DataFrame` that has statistics for a DataFrame.

        Only summarized numeric datatypes at the moments and returns nulls
        for non-numeric datatypes.

        The output format is modeled after pandas.

        Returns:
        -------
        DataFrame
            A summary DataFrame containing statistics.
        """
        return DataFrame(self.df.describe())

    def schema(self) -> pa.Schema:
        """Return the `pyarrow.Schema` describing the output of this DataFrame.

        The output schema contains information on the name, data type, and
        nullability for each column.

        Returns:
        -------
        pa.Schema
            Describing schema of the DataFrame
        """
        return self.df.schema()

    def select_columns(self, *args: str) -> DataFrame:
        """Filter the DataFrame by columns.

        Returns:
        -------
        DataFrame
            DataFrame only containing the specified columns.
        """
        return DataFrame(self.df.select_columns(*args))

    def select(self, *args: Expr) -> DataFrame:
        """Project arbitrary expressions (like SQL SELECT expressions) into a new `DataFrame`.

        Returns:
        -------
        DataFrame
            DataFrame after projection. It has one column for each expression.
        """
        args = [arg.expr for arg in args]
        return DataFrame(self.df.select(*args))

    def filter(self, predicate: Expr) -> DataFrame:
        """Return a DataFrame for which `predicate` evaluates to `True`.

        Rows for which `predicate` evaluates to `False` or `None` are filtered out.

        Parameters
        ----------
        predicate : Expr
            Predicate expression to filter the DataFrame.

        Returns:
        -------
        DataFrame
            DataFrame after filtering.
        """
        return DataFrame(self.df.filter(predicate.expr))

    def with_column(self, name: str, expr: Expr) -> DataFrame:
        """Add an additional column to the DataFrame.

        Parameters
        ----------
        name : str
            Name of the column to add.
        expr : Expr
            Expression to compute the column.

        Returns:
        -------
        DataFrame
            DataFrame with the new column.
        """
        return DataFrame(self.df.with_column(name, expr.expr))

    def with_column_renamed(self, old_name: str, new_name: str) -> DataFrame:
        """Rename one column by applying a new projection.

        This is a no-op if the column to be renamed does not exist.

        The method supports case sensitive rename with wrapping column name
        into one the following symbols (" or ' or `).

        Parameters
        ----------
        old_name : str
            Old column name.
        new_name : str
            New column name.

        Returns:
        -------
        DataFrame
            DataFrame with the column renamed.
        """
        return DataFrame(self.df.with_column_renamed(old_name, new_name))

    def aggregate(self, group_by: list[Expr], aggs: list[Expr]) -> DataFrame:
        """Return a new `DataFrame` that aggregates the rows of the current DataFrame.

        First optionally grouping by the given expressions.

        Parameters
        ----------
        group_by : list[Expr]
            List of expressions to group by.
        aggs : list[Expr]
            List of expressions to aggregate.

        Returns:
        -------
        DataFrame
            DataFrame after aggregation.
        """
        group_by = [e.expr for e in group_by]
        aggs = [e.expr for e in aggs]
        return DataFrame(self.df.aggregate(group_by, aggs))

    def sort(self, *exprs: Expr) -> DataFrame:
        """Sort the DataFrame by the specified sorting expressions.

        Note that any expression can be turned into a sort expression by
        calling its `sort`  method.

        Returns:
        -------
        DataFrame
            DataFrame after sorting.
        """
        exprs = [expr.expr for expr in exprs]
        return DataFrame(self.df.sort(*exprs))

    def limit(self, count: int, offset: int = 0) -> DataFrame:
        """Return a new `DataFrame` with a limited number of rows.

        Parameters
        ----------
        count : int
            Number of rows to limit the DataFrame to.
        offset : int, optional
            Number of rows to skip, by default 0

        Returns:
        -------
        DataFrame
            DataFrame after limiting.
        """
        return DataFrame(self.df.limit(count, offset))

    def collect(self) -> list[pa.RecordBatch]:
        """Execute this `DataFrame` and collect `pyarrow.RecordBatch`es into memory.

        Prior to calling `collect`, modifying a DataFrme simply updates a plan
        (no actual computation is performed). Calling `collect` triggers the
        computation.

        Returns:
        -------
        list[pa.RecordBatch]
            List of `pyarrow.RecordBatch`es collected from the DataFrame.
        """
        return self.df.collect()

    def cache(self) -> DataFrame:
        """Cache the DataFrame as a memory table.

        Returns:
        -------
        DataFrame
            Cached DataFrame.
        """
        return DataFrame(self.df.cache())

    def collect_partitioned(self) -> list[list[pa.RecordBatch]]:
        """Execute this DataFrame and collect all results into a list of list of `pyarrow.RecordBatch`es maintaining the input partitioning.

        Returns:
        -------
        list[list[pa.RecordBatch]]
            List of list of `pyarrow.RecordBatch`es collected from the DataFrame.
        """
        return self.df.collect_partitioned()

    def show(self, num: int = 20) -> None:
        """Execute the DataFrame and print the result to the console.

        Parameters
        ----------
        num : int, optional
            Number of lines to show, by default 20
        """
        self.df.show(num)

    def distinct(self) -> DataFrame:
        """Return a new `DataFrame` with all duplicated rows removed.

        Returns:
        -------
        DataFrame
            DataFrame after removing duplicates.
        """
        return DataFrame(self.df.distinct())

    def join(
        self,
        right: DataFrame,
        join_keys: tuple[list[str], list[str]],
        how: str,
    ) -> DataFrame:
        """Join this `DataFrame` with another `DataFrame`  using explicitly specified columns.

        Parameters
        ----------
        right : DataFrame
            Other DataFrame to join with.
        join_keys : tuple[list[str], list[str]]
            Tuple of two lists of column names to join on.
        how : str
            Type of join to perform. Supported types are "inner", "left", "right", "full", "semi", "anti".

        Returns:
        -------
        DataFrame
            DataFrame after join.
        """
        return DataFrame(self.df.join(right.df, join_keys, how))

    def explain(self, verbose: bool = False, analyze: bool = False) -> DataFrame:
        """Return a DataFrame with the explanation of its plan so far.

        If `analyze` is specified, runs the plan and reports metrics.

        Parameters
        ----------
        verbose : bool, optional
            If `True`, more details will be included, by default False
        analyze : bool, optional
            If `True`, the plan will run and metrics reported, by default False

        Returns:
        -------
        DataFrame
            DataFrame with the explanation of its plan.
        """
        return DataFrame(self.df.explain(verbose, analyze))

    def logical_plan(self) -> LogicalPlan:
        """Return the unoptimized `LogicalPlan` that comprises this `DataFrame`.

        Returns:
        -------
        LogicalPlan
            Unoptimized logical plan.
        """
        return self.df.logical_plan()

    def optimized_logical_plan(self) -> LogicalPlan:
        """Return the optimized `LogicalPlan` that comprises this `DataFrame`.

        Returns:
        -------
        LogicalPlan
            Optimized logical plan.
        """
        return self.df.optimized_logical_plan()

    def execution_plan(self) -> ExecutionPlan:
        """Return the execution/physical plan that comprises this `DataFrame`.

        Returns:
        -------
        ExecutionPlan
            Execution plan.
        """
        return self.df.execution_plan()

    def repartition(self, num: int) -> DataFrame:
        """Repartition a DataFrame into `num` partitions.

        The batches allocation uses a round-robin algorithm.

        Parameters
        ----------
        num : int
            Number of partitions to repartition the DataFrame into.

        Returns:
        -------
        DataFrame
            Repartitioned DataFrame.
        """
        return DataFrame(self.df.repartition(num))

    def repartition_by_hash(self, *args: Expr, num: int) -> DataFrame:
        """Repartition a DataFrame into `num` partitions using a hash partitioning scheme.

        Parameters
        ----------
        num : int
            Number of partitions to repartition the DataFrame into.

        Returns:
        -------
        DataFrame
            Repartitioned DataFrame.
        """
        args = [expr.expr for expr in args]
        return DataFrame(self.df.repartition_by_hash(*args, num=num))

    def union(self, other: DataFrame, distinct: bool = False) -> DataFrame:
        """Calculate the union of two `DataFrame`s.

        The two `DataFrame`s must have exactly the same schema.

        Parameters
        ----------
        other : DataFrame
            DataFrame to union with.
        distinct : bool, optional
            If `True`, duplicate rows will be removed, by default False

        Returns:
        -------
        DataFrame
            DataFrame after union.
        """
        return DataFrame(self.df.union(other.df, distinct))

    def union_distinct(self, other: DataFrame) -> DataFrame:
        """Calculate the distinct union of two `DataFrame`s.

        The two `DataFrame`s must have exactly the same schema.
        Any duplicate rows are discarded.

        Parameters
        ----------
        other : DataFrame
            DataFrame to union with.

        Returns:
        -------
        DataFrame
            DataFrame after union.
        """
        return DataFrame(self.df.union_distinct(other.df))

    def intersect(self, other: DataFrame) -> DataFrame:
        """Calculate the intersection of two `DataFrame`s.

        The two `DataFrame`s must have exactly the same schema.

        Parameters
        ----------
        other : DataFrame
            DataFrame to intersect with.

        Returns:
        -------
        DataFrame
            DataFrame after intersection.
        """
        return DataFrame(self.df.intersect(other.df))

    def except_all(self, other: DataFrame) -> DataFrame:
        """Calculate the exception of two `DataFrame`s.

        The two `DataFrame`s must have exactly the same schema.

        Parameters
        ----------
        other : DataFrame
            DataFrame to calculate exception with.

        Returns:
        -------
        DataFrame
            DataFrame after exception.
        """
        return DataFrame(self.df.except_all(other.df))

    def write_csv(self, path: str | pathlib.Path) -> None:
        """Execute the `DataFrame`  and write the results to a CSV file.

        Parameters
        ----------
        path : str
            Path of the CSV file to write.
        """
        self.df.write_csv(str(path))

    def write_parquet(
        self,
        path: str | pathlib.Path,
        compression: str = "uncompressed",
        compression_level: int | None = None,
    ) -> None:
        """Execute the `DataFrame` and write the results to a Parquet file.

        Parameters
        ----------
        path : str
            Path of the Parquet file to write.
        compression : str, optional
            Compression type to use, by default "uncompressed"
        compression_level : int | None, optional
            Compression level to use, by default None
        """
        self.df.write_parquet(str(path), compression, compression_level)

    def write_json(self, path: str | pathlib.Path) -> None:
        """Execute the `DataFrame` and write the results to a JSON file.

        Parameters
        ----------
        path : str
            Path of the JSON file to write.
        """
        self.df.write_json(str(path))

    def to_arrow_table(self) -> pa.Table:
        """Execute the `DataFrame` and convert it into an Arrow Table.

        Returns:
        -------
        pa.Table
            Arrow Table.
        """
        return self.df.to_arrow_table()

    def execute_stream(self) -> RecordBatchStream:
        """Executes this DataFrame and returns a stream over a single partition."""
        return RecordBatchStream(self.df.execute_stream())

    def execute_stream_partitioned(self) -> list[RecordBatchStream]:
        """Executes this DataFrame and returns a stream for each partition."""
        streams = self.df.execute_stream_partitioned()
        return [RecordBatchStream(rbs) for rbs in streams]

    def to_pandas(self) -> pd.DataFrame:
        """Execute the `DataFrame` and convert it into a Pandas DataFrame.

        Returns:
        -------
        pd.DataFrame
            Pandas DataFrame.
        """
        return self.df.to_pandas()

    def to_pylist(self) -> list[dict[str, Any]]:
        """Execute the `DataFrame` and convert it into a list of dictionaries.

        Returns:
        -------
        list[dict[str, Any]]
            List of dictionaries.
        """
        return self.df.to_pylist()

    def to_pydict(self) -> dict[str, list[Any]]:
        """Execute the `DataFrame` and convert it into a dictionary of lists.

        Returns:
        -------
        dict[str, list[Any]]
            Dictionary of lists.
        """
        return self.df.to_pydict()

    def to_polars(self) -> pl.DataFrame:
        """Execute the `DataFrame` and convert it into a Polars DataFrame.

        Returns:
        -------
        pl.DataFrame
            Polars DataFrame.
        """
        return self.df.to_polars()

    def count(self) -> int:
        """Return the total number of rows in this `DataFrame`.

        Note that this method will actually run a plan to calculate the
        count, which may be slow for large or complicated DataFrames.

        Returns:
        -------
        int
            Number of rows in the DataFrame.
        """
        return self.df.count()

    @deprecated("Use :func:`unnest_columns` instead.")
    def unnest_column(self, column: str, preserve_nulls: bool = True) -> DataFrame:
        """See ``unnest_columns``."""
        return DataFrame(self.df.unnest_column(column, preserve_nulls=preserve_nulls))

    def unnest_columns(
        self, columns: list[str], preserve_nulls: bool = True
    ) -> DataFrame:
        """Expand columns of arrays into a single row per array element."""
        return DataFrame(self.df.unnest_columns(columns, preserve_nulls=preserve_nulls))
