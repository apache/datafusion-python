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

"""Session Context and it's associated configuration."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Protocol

import pyarrow as pa

try:
    from warnings import deprecated  # Python 3.13+
except ImportError:
    from typing_extensions import deprecated  # Python 3.12

from datafusion.catalog import Catalog, CatalogProvider, Table
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr, SortExpr, sort_list_to_raw_sort_list
from datafusion.record_batch import RecordBatchStream
from datafusion.user_defined import AggregateUDF, ScalarUDF, TableFunction, WindowUDF

from ._internal import RuntimeEnvBuilder as RuntimeEnvBuilderInternal
from ._internal import SessionConfig as SessionConfigInternal
from ._internal import SessionContext as SessionContextInternal
from ._internal import SQLOptions as SQLOptionsInternal

if TYPE_CHECKING:
    import pathlib

    import pandas as pd
    import polars as pl

    from datafusion.plan import ExecutionPlan, LogicalPlan


class ArrowStreamExportable(Protocol):
    """Type hint for object exporting Arrow C Stream via Arrow PyCapsule Interface.

    https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
    """

    def __arrow_c_stream__(  # noqa: D105
        self, requested_schema: object | None = None
    ) -> object: ...


class ArrowArrayExportable(Protocol):
    """Type hint for object exporting Arrow C Array via Arrow PyCapsule Interface.

    https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html
    """

    def __arrow_c_array__(  # noqa: D105
        self, requested_schema: object | None = None
    ) -> tuple[object, object]: ...


class TableProviderExportable(Protocol):
    """Type hint for object that has __datafusion_table_provider__ PyCapsule.

    https://datafusion.apache.org/python/user-guide/io/table_provider.html
    """

    def __datafusion_table_provider__(self) -> object: ...  # noqa: D105


class CatalogProviderExportable(Protocol):
    """Type hint for object that has __datafusion_catalog_provider__ PyCapsule.

    https://docs.rs/datafusion/latest/datafusion/catalog/trait.CatalogProvider.html
    """

    def __datafusion_catalog_provider__(self) -> object: ...  # noqa: D105


class SessionConfig:
    """Session configuration options."""

    def __init__(self, config_options: dict[str, str] | None = None) -> None:
        """Create a new :py:class:`SessionConfig` with the given configuration options.

        Args:
            config_options: Configuration options.
        """
        self.config_internal = SessionConfigInternal(config_options)

    def with_create_default_catalog_and_schema(
        self, enabled: bool = True
    ) -> SessionConfig:
        """Control if the default catalog and schema will be automatically created.

        Args:
            enabled: Whether the default catalog and schema will be
                automatically created.

        Returns:
            A new :py:class:`SessionConfig` object with the updated setting.
        """
        self.config_internal = (
            self.config_internal.with_create_default_catalog_and_schema(enabled)
        )
        return self

    def with_default_catalog_and_schema(
        self, catalog: str, schema: str
    ) -> SessionConfig:
        """Select a name for the default catalog and schema.

        Args:
            catalog: Catalog name.
            schema: Schema name.

        Returns:
            A new :py:class:`SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_default_catalog_and_schema(
            catalog, schema
        )
        return self

    def with_information_schema(self, enabled: bool = True) -> SessionConfig:
        """Enable or disable the inclusion of ``information_schema`` virtual tables.

        Args:
            enabled: Whether to include ``information_schema`` virtual tables.

        Returns:
            A new :py:class:`SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_information_schema(enabled)
        return self

    def with_batch_size(self, batch_size: int) -> SessionConfig:
        """Customize batch size.

        Args:
            batch_size: Batch size.

        Returns:
            A new :py:class:`SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_batch_size(batch_size)
        return self

    def with_target_partitions(self, target_partitions: int) -> SessionConfig:
        """Customize the number of target partitions for query execution.

        Increasing partitions can increase concurrency.

        Args:
            target_partitions: Number of target partitions.

        Returns:
            A new :py:class:`SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_target_partitions(
            target_partitions
        )
        return self

    def with_repartition_aggregations(self, enabled: bool = True) -> SessionConfig:
        """Enable or disable the use of repartitioning for aggregations.

        Enabling this improves parallelism.

        Args:
            enabled: Whether to use repartitioning for aggregations.

        Returns:
            A new :py:class:`SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_repartition_aggregations(
            enabled
        )
        return self

    def with_repartition_joins(self, enabled: bool = True) -> SessionConfig:
        """Enable or disable the use of repartitioning for joins to improve parallelism.

        Args:
            enabled: Whether to use repartitioning for joins.

        Returns:
            A new :py:class:`SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_repartition_joins(enabled)
        return self

    def with_repartition_windows(self, enabled: bool = True) -> SessionConfig:
        """Enable or disable the use of repartitioning for window functions.

        This may improve parallelism.

        Args:
            enabled: Whether to use repartitioning for window functions.

        Returns:
            A new :py:class:`SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_repartition_windows(enabled)
        return self

    def with_repartition_sorts(self, enabled: bool = True) -> SessionConfig:
        """Enable or disable the use of repartitioning for window functions.

        This may improve parallelism.

        Args:
            enabled: Whether to use repartitioning for window functions.

        Returns:
            A new :py:class:`SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_repartition_sorts(enabled)
        return self

    def with_repartition_file_scans(self, enabled: bool = True) -> SessionConfig:
        """Enable or disable the use of repartitioning for file scans.

        Args:
            enabled: Whether to use repartitioning for file scans.

        Returns:
            A new :py:class:`SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_repartition_file_scans(enabled)
        return self

    def with_repartition_file_min_size(self, size: int) -> SessionConfig:
        """Set minimum file range size for repartitioning scans.

        Args:
            size: Minimum file range size.

        Returns:
            A new :py:class:`SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_repartition_file_min_size(size)
        return self

    def with_parquet_pruning(self, enabled: bool = True) -> SessionConfig:
        """Enable or disable the use of pruning predicate for parquet readers.

        Pruning predicates will enable the reader to skip row groups.

        Args:
            enabled: Whether to use pruning predicate for parquet readers.

        Returns:
            A new :py:class:`SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_parquet_pruning(enabled)
        return self

    def set(self, key: str, value: str) -> SessionConfig:
        """Set a configuration option.

        Args:
        key: Option key.
        value: Option value.

        Returns:
            A new :py:class:`SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.set(key, value)
        return self


class RuntimeEnvBuilder:
    """Runtime configuration options."""

    def __init__(self) -> None:
        """Create a new :py:class:`RuntimeEnvBuilder` with default values."""
        self.config_internal = RuntimeEnvBuilderInternal()

    def with_disk_manager_disabled(self) -> RuntimeEnvBuilder:
        """Disable the disk manager, attempts to create temporary files will error.

        Returns:
            A new :py:class:`RuntimeEnvBuilder` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_disk_manager_disabled()
        return self

    def with_disk_manager_os(self) -> RuntimeEnvBuilder:
        """Use the operating system's temporary directory for disk manager.

        Returns:
            A new :py:class:`RuntimeEnvBuilder` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_disk_manager_os()
        return self

    def with_disk_manager_specified(
        self, *paths: str | pathlib.Path
    ) -> RuntimeEnvBuilder:
        """Use the specified paths for the disk manager's temporary files.

        Args:
            paths: Paths to use for the disk manager's temporary files.

        Returns:
            A new :py:class:`RuntimeEnvBuilder` object with the updated setting.
        """
        paths_list = [str(p) for p in paths]
        self.config_internal = self.config_internal.with_disk_manager_specified(
            paths_list
        )
        return self

    def with_unbounded_memory_pool(self) -> RuntimeEnvBuilder:
        """Use an unbounded memory pool.

        Returns:
            A new :py:class:`RuntimeEnvBuilder` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_unbounded_memory_pool()
        return self

    def with_fair_spill_pool(self, size: int) -> RuntimeEnvBuilder:
        """Use a fair spill pool with the specified size.

        This pool works best when you know beforehand the query has multiple spillable
        operators that will likely all need to spill. Sometimes it will cause spills
        even when there was sufficient memory (reserved for other operators) to avoid
        doing so::

            ┌───────────────────────z──────────────────────z───────────────┐
            │                       z                      z               │
            │                       z                      z               │
            │       Spillable       z       Unspillable    z     Free      │
            │        Memory         z        Memory        z    Memory     │
            │                       z                      z               │
            │                       z                      z               │
            └───────────────────────z──────────────────────z───────────────┘

        Args:
            size: Size of the memory pool in bytes.

        Returns:
            A new :py:class:`RuntimeEnvBuilder` object with the updated setting.

        Examples usage::

            config = RuntimeEnvBuilder().with_fair_spill_pool(1024)
        """
        self.config_internal = self.config_internal.with_fair_spill_pool(size)
        return self

    def with_greedy_memory_pool(self, size: int) -> RuntimeEnvBuilder:
        """Use a greedy memory pool with the specified size.

        This pool works well for queries that do not need to spill or have a single
        spillable operator. See :py:func:`with_fair_spill_pool` if there are
        multiple spillable operators that all will spill.

        Args:
            size: Size of the memory pool in bytes.

        Returns:
            A new :py:class:`RuntimeEnvBuilder` object with the updated setting.

        Example usage::

            config = RuntimeEnvBuilder().with_greedy_memory_pool(1024)
        """
        self.config_internal = self.config_internal.with_greedy_memory_pool(size)
        return self

    def with_temp_file_path(self, path: str | pathlib.Path) -> RuntimeEnvBuilder:
        """Use the specified path to create any needed temporary files.

        Args:
            path: Path to use for temporary files.

        Returns:
            A new :py:class:`RuntimeEnvBuilder` object with the updated setting.

        Example usage::

            config = RuntimeEnvBuilder().with_temp_file_path("/tmp")
        """
        self.config_internal = self.config_internal.with_temp_file_path(str(path))
        return self


@deprecated("Use `RuntimeEnvBuilder` instead.")
class RuntimeConfig(RuntimeEnvBuilder):
    """See `RuntimeEnvBuilder`."""


class SQLOptions:
    """Options to be used when performing SQL queries."""

    def __init__(self) -> None:
        """Create a new :py:class:`SQLOptions` with default values.

        The default values are:
        - DDL commands are allowed
        - DML commands are allowed
        - Statements are allowed
        """
        self.options_internal = SQLOptionsInternal()

    def with_allow_ddl(self, allow: bool = True) -> SQLOptions:
        """Should DDL (Data Definition Language) commands be run?

        Examples of DDL commands include ``CREATE TABLE`` and ``DROP TABLE``.

        Args:
            allow: Allow DDL commands to be run.

        Returns:
            A new :py:class:`SQLOptions` object with the updated setting.

        Example usage::

            options = SQLOptions().with_allow_ddl(True)
        """
        self.options_internal = self.options_internal.with_allow_ddl(allow)
        return self

    def with_allow_dml(self, allow: bool = True) -> SQLOptions:
        """Should DML (Data Manipulation Language) commands be run?

        Examples of DML commands include ``INSERT INTO`` and ``DELETE``.

        Args:
            allow: Allow DML commands to be run.

        Returns:
            A new :py:class:`SQLOptions` object with the updated setting.

        Example usage::

            options = SQLOptions().with_allow_dml(True)
        """
        self.options_internal = self.options_internal.with_allow_dml(allow)
        return self

    def with_allow_statements(self, allow: bool = True) -> SQLOptions:
        """Should statements such as ``SET VARIABLE`` and ``BEGIN TRANSACTION`` be run?

        Args:
            allow: Allow statements to be run.

        Returns:
            A new :py:class:SQLOptions` object with the updated setting.

        Example usage::

            options = SQLOptions().with_allow_statements(True)
        """
        self.options_internal = self.options_internal.with_allow_statements(allow)
        return self


class SessionContext:
    """This is the main interface for executing queries and creating DataFrames.

    See :ref:`user_guide_concepts` in the online documentation for more information.
    """

    def __init__(
        self,
        config: SessionConfig | None = None,
        runtime: RuntimeEnvBuilder | None = None,
    ) -> None:
        """Main interface for executing queries with DataFusion.

        Maintains the state of the connection between a user and an instance
        of the connection between a user and an instance of the DataFusion
        engine.

        Args:
            config: Session configuration options.
            runtime: Runtime configuration options.

        Example usage:

        The following example demonstrates how to use the context to execute
        a query against a CSV data source using the :py:class:`DataFrame` API::

            from datafusion import SessionContext

            ctx = SessionContext()
            df = ctx.read_csv("data.csv")
        """
        config = config.config_internal if config is not None else None
        runtime = runtime.config_internal if runtime is not None else None

        self.ctx = SessionContextInternal(config, runtime)

    def __repr__(self) -> str:
        """Print a string representation of the Session Context."""
        return self.ctx.__repr__()

    @classmethod
    def global_ctx(cls) -> SessionContext:
        """Retrieve the global context as a `SessionContext` wrapper.

        Returns:
            A `SessionContext` object that wraps the global `SessionContextInternal`.
        """
        internal_ctx = SessionContextInternal.global_ctx()
        wrapper = cls()
        wrapper.ctx = internal_ctx
        return wrapper

    def enable_url_table(self) -> SessionContext:
        """Control if local files can be queried as tables.

        Returns:
            A new :py:class:`SessionContext` object with url table enabled.
        """
        klass = self.__class__
        obj = klass.__new__(klass)
        obj.ctx = self.ctx.enable_url_table()
        return obj

    def register_object_store(
        self, schema: str, store: Any, host: str | None = None
    ) -> None:
        """Add a new object store into the session.

        Args:
            schema: The data source schema.
            store: The :py:class:`~datafusion.object_store.ObjectStore` to register.
            host: URL for the host.
        """
        self.ctx.register_object_store(schema, store, host)

    def register_listing_table(
        self,
        name: str,
        path: str | pathlib.Path,
        table_partition_cols: list[tuple[str, str | pa.DataType]] | None = None,
        file_extension: str = ".parquet",
        schema: pa.Schema | None = None,
        file_sort_order: list[list[Expr | SortExpr]] | None = None,
    ) -> None:
        """Register multiple files as a single table.

        Registers a :py:class:`~datafusion.catalog.Table` that can assemble multiple
        files from locations in an :py:class:`~datafusion.object_store.ObjectStore`
        instance.

        Args:
            name: Name of the resultant table.
            path: Path to the file to register.
            table_partition_cols: Partition columns.
            file_extension: File extension of the provided table.
            schema: The data source schema.
            file_sort_order: Sort order for the file.
        """
        if table_partition_cols is None:
            table_partition_cols = []
        table_partition_cols = self._convert_table_partition_cols(table_partition_cols)
        file_sort_order_raw = (
            [sort_list_to_raw_sort_list(f) for f in file_sort_order]
            if file_sort_order is not None
            else None
        )
        self.ctx.register_listing_table(
            name,
            str(path),
            table_partition_cols,
            file_extension,
            schema,
            file_sort_order_raw,
        )

    def sql(self, query: str, options: SQLOptions | None = None) -> DataFrame:
        """Create a :py:class:`~datafusion.DataFrame` from SQL query text.

        Note: This API implements DDL statements such as ``CREATE TABLE`` and
        ``CREATE VIEW`` and DML statements such as ``INSERT INTO`` with in-memory
        default implementation.See
        :py:func:`~datafusion.context.SessionContext.sql_with_options`.

        Args:
            query: SQL query text.
            options: If provided, the query will be validated against these options.

        Returns:
            DataFrame representation of the SQL query.
        """
        if options is None:
            return DataFrame(self.ctx.sql(query))
        return DataFrame(self.ctx.sql_with_options(query, options.options_internal))

    def sql_with_options(self, query: str, options: SQLOptions) -> DataFrame:
        """Create a :py:class:`~datafusion.dataframe.DataFrame` from SQL query text.

        This function will first validate that the query is allowed by the
        provided options.

        Args:
            query: SQL query text.
            options: SQL options.

        Returns:
            DataFrame representation of the SQL query.
        """
        return self.sql(query, options)

    def create_dataframe(
        self,
        partitions: list[list[pa.RecordBatch]],
        name: str | None = None,
        schema: pa.Schema | None = None,
    ) -> DataFrame:
        """Create and return a dataframe using the provided partitions.

        Args:
            partitions: :py:class:`pa.RecordBatch` partitions to register.
            name: Resultant dataframe name.
            schema: Schema for the partitions.

        Returns:
            DataFrame representation of the SQL query.
        """
        return DataFrame(self.ctx.create_dataframe(partitions, name, schema))

    def create_dataframe_from_logical_plan(self, plan: LogicalPlan) -> DataFrame:
        """Create a :py:class:`~datafusion.dataframe.DataFrame` from an existing plan.

        Args:
            plan: Logical plan.

        Returns:
            DataFrame representation of the logical plan.
        """
        return DataFrame(self.ctx.create_dataframe_from_logical_plan(plan._raw_plan))

    def from_pylist(
        self, data: list[dict[str, Any]], name: str | None = None
    ) -> DataFrame:
        """Create a :py:class:`~datafusion.dataframe.DataFrame` from a list.

        Args:
            data: List of dictionaries.
            name: Name of the DataFrame.

        Returns:
            DataFrame representation of the list of dictionaries.
        """
        return DataFrame(self.ctx.from_pylist(data, name))

    def from_pydict(
        self, data: dict[str, list[Any]], name: str | None = None
    ) -> DataFrame:
        """Create a :py:class:`~datafusion.dataframe.DataFrame` from a dictionary.

        Args:
            data: Dictionary of lists.
            name: Name of the DataFrame.

        Returns:
            DataFrame representation of the dictionary of lists.
        """
        return DataFrame(self.ctx.from_pydict(data, name))

    def from_arrow(
        self,
        data: ArrowStreamExportable | ArrowArrayExportable,
        name: str | None = None,
    ) -> DataFrame:
        """Create a :py:class:`~datafusion.dataframe.DataFrame` from an Arrow source.

        The Arrow data source can be any object that implements either
        ``__arrow_c_stream__`` or ``__arrow_c_array__``. For the latter, it must return
        a struct array.

        Arrow data can be Polars, Pandas, Pyarrow etc.

        Args:
            data: Arrow data source.
            name: Name of the DataFrame.

        Returns:
            DataFrame representation of the Arrow table.
        """
        return DataFrame(self.ctx.from_arrow(data, name))

    @deprecated("Use ``from_arrow`` instead.")
    def from_arrow_table(self, data: pa.Table, name: str | None = None) -> DataFrame:
        """Create a :py:class:`~datafusion.dataframe.DataFrame` from an Arrow table.

        This is an alias for :py:func:`from_arrow`.
        """
        return self.from_arrow(data, name)

    def from_pandas(self, data: pd.DataFrame, name: str | None = None) -> DataFrame:
        """Create a :py:class:`~datafusion.dataframe.DataFrame` from a Pandas DataFrame.

        Args:
            data: Pandas DataFrame.
            name: Name of the DataFrame.

        Returns:
            DataFrame representation of the Pandas DataFrame.
        """
        return DataFrame(self.ctx.from_pandas(data, name))

    def from_polars(self, data: pl.DataFrame, name: str | None = None) -> DataFrame:
        """Create a :py:class:`~datafusion.dataframe.DataFrame` from a Polars DataFrame.

        Args:
            data: Polars DataFrame.
            name: Name of the DataFrame.

        Returns:
            DataFrame representation of the Polars DataFrame.
        """
        return DataFrame(self.ctx.from_polars(data, name))

    # https://github.com/apache/datafusion-python/pull/1016#discussion_r1983239116
    # is the discussion on how we arrived at adding register_view
    def register_view(self, name: str, df: DataFrame) -> None:
        """Register a :py:class: `~datafusion.detaframe.DataFrame` as a view.

        Args:
            name (str): The name to register the view under.
            df (DataFrame): The DataFrame to be converted into a view and registered.
        """
        view = df.into_view()
        self.ctx.register_table(name, view)

    def register_table(self, name: str, table: Table) -> None:
        """Register a :py:class: `~datafusion.catalog.Table` as a table.

        The registered table can be referenced from SQL statement executed against.

        Args:
            name: Name of the resultant table.
            table: DataFusion table to add to the session context.
        """
        self.ctx.register_table(name, table.table)

    def deregister_table(self, name: str) -> None:
        """Remove a table from the session."""
        self.ctx.deregister_table(name)

    def catalog_names(self) -> set[str]:
        """Returns the list of catalogs in this context."""
        return self.ctx.catalog_names()

    def register_catalog_provider(
        self, name: str, provider: CatalogProviderExportable | CatalogProvider | Catalog
    ) -> None:
        """Register a catalog provider."""
        if isinstance(provider, Catalog):
            self.ctx.register_catalog_provider(name, provider.catalog)
        else:
            self.ctx.register_catalog_provider(name, provider)

    def register_table_provider(
        self, name: str, provider: TableProviderExportable
    ) -> None:
        """Register a table provider.

        This table provider must have a method called ``__datafusion_table_provider__``
        which returns a PyCapsule that exposes a ``FFI_TableProvider``.
        """
        self.ctx.register_table_provider(name, provider)

    def register_udtf(self, func: TableFunction) -> None:
        """Register a user defined table function."""
        self.ctx.register_udtf(func._udtf)

    def register_record_batches(
        self, name: str, partitions: list[list[pa.RecordBatch]]
    ) -> None:
        """Register record batches as a table.

        This function will convert the provided partitions into a table and
        register it into the session using the given name.

        Args:
            name: Name of the resultant table.
            partitions: Record batches to register as a table.
        """
        self.ctx.register_record_batches(name, partitions)

    def register_parquet(
        self,
        name: str,
        path: str | pathlib.Path,
        table_partition_cols: list[tuple[str, str | pa.DataType]] | None = None,
        parquet_pruning: bool = True,
        file_extension: str = ".parquet",
        skip_metadata: bool = True,
        schema: pa.Schema | None = None,
        file_sort_order: list[list[SortExpr]] | None = None,
    ) -> None:
        """Register a Parquet file as a table.

        The registered table can be referenced from SQL statement executed
        against this context.

        Args:
            name: Name of the table to register.
            path: Path to the Parquet file.
            table_partition_cols: Partition columns.
            parquet_pruning: Whether the parquet reader should use the
                predicate to prune row groups.
            file_extension: File extension; only files with this extension are
                selected for data input.
            skip_metadata: Whether the parquet reader should skip any metadata
                that may be in the file schema. This can help avoid schema
                conflicts due to metadata.
            schema: The data source schema.
            file_sort_order: Sort order for the file.
        """
        if table_partition_cols is None:
            table_partition_cols = []
        table_partition_cols = self._convert_table_partition_cols(table_partition_cols)
        self.ctx.register_parquet(
            name,
            str(path),
            table_partition_cols,
            parquet_pruning,
            file_extension,
            skip_metadata,
            schema,
            [sort_list_to_raw_sort_list(exprs) for exprs in file_sort_order]
            if file_sort_order is not None
            else None,
        )

    def register_csv(
        self,
        name: str,
        path: str | pathlib.Path | list[str | pathlib.Path],
        schema: pa.Schema | None = None,
        has_header: bool = True,
        delimiter: str = ",",
        schema_infer_max_records: int = 1000,
        file_extension: str = ".csv",
        file_compression_type: str | None = None,
    ) -> None:
        """Register a CSV file as a table.

        The registered table can be referenced from SQL statement executed against.

        Args:
            name: Name of the table to register.
            path: Path to the CSV file. It also accepts a list of Paths.
            schema: An optional schema representing the CSV file. If None, the
                CSV reader will try to infer it based on data in file.
            has_header: Whether the CSV file have a header. If schema inference
                is run on a file with no headers, default column names are
                created.
            delimiter: An optional column delimiter.
            schema_infer_max_records: Maximum number of rows to read from CSV
                files for schema inference if needed.
            file_extension: File extension; only files with this extension are
                selected for data input.
            file_compression_type: File compression type.
        """
        path = [str(p) for p in path] if isinstance(path, list) else str(path)

        self.ctx.register_csv(
            name,
            path,
            schema,
            has_header,
            delimiter,
            schema_infer_max_records,
            file_extension,
            file_compression_type,
        )

    def register_json(
        self,
        name: str,
        path: str | pathlib.Path,
        schema: pa.Schema | None = None,
        schema_infer_max_records: int = 1000,
        file_extension: str = ".json",
        table_partition_cols: list[tuple[str, str | pa.DataType]] | None = None,
        file_compression_type: str | None = None,
    ) -> None:
        """Register a JSON file as a table.

        The registered table can be referenced from SQL statement executed
        against this context.

        Args:
            name: Name of the table to register.
            path: Path to the JSON file.
            schema: The data source schema.
            schema_infer_max_records: Maximum number of rows to read from JSON
                files for schema inference if needed.
            file_extension: File extension; only files with this extension are
                selected for data input.
            table_partition_cols: Partition columns.
            file_compression_type: File compression type.
        """
        if table_partition_cols is None:
            table_partition_cols = []
        table_partition_cols = self._convert_table_partition_cols(table_partition_cols)
        self.ctx.register_json(
            name,
            str(path),
            schema,
            schema_infer_max_records,
            file_extension,
            table_partition_cols,
            file_compression_type,
        )

    def register_avro(
        self,
        name: str,
        path: str | pathlib.Path,
        schema: pa.Schema | None = None,
        file_extension: str = ".avro",
        table_partition_cols: list[tuple[str, str | pa.DataType]] | None = None,
    ) -> None:
        """Register an Avro file as a table.

        The registered table can be referenced from SQL statement executed against
        this context.

        Args:
            name: Name of the table to register.
            path: Path to the Avro file.
            schema: The data source schema.
            file_extension: File extension to select.
            table_partition_cols:  Partition columns.
        """
        if table_partition_cols is None:
            table_partition_cols = []
        table_partition_cols = self._convert_table_partition_cols(table_partition_cols)
        self.ctx.register_avro(
            name, str(path), schema, file_extension, table_partition_cols
        )

    def register_dataset(self, name: str, dataset: pa.dataset.Dataset) -> None:
        """Register a :py:class:`pa.dataset.Dataset` as a table.

        Args:
            name: Name of the table to register.
            dataset: PyArrow dataset.
        """
        self.ctx.register_dataset(name, dataset)

    def register_udf(self, udf: ScalarUDF) -> None:
        """Register a user-defined function (UDF) with the context."""
        self.ctx.register_udf(udf._udf)

    def register_udaf(self, udaf: AggregateUDF) -> None:
        """Register a user-defined aggregation function (UDAF) with the context."""
        self.ctx.register_udaf(udaf._udaf)

    def register_udwf(self, udwf: WindowUDF) -> None:
        """Register a user-defined window function (UDWF) with the context."""
        self.ctx.register_udwf(udwf._udwf)

    def catalog(self, name: str = "datafusion") -> Catalog:
        """Retrieve a catalog by name."""
        return Catalog(self.ctx.catalog(name))

    @deprecated(
        "Use the catalog provider interface ``SessionContext.Catalog`` to "
        "examine available catalogs, schemas and tables"
    )
    def tables(self) -> set[str]:
        """Deprecated."""
        return self.ctx.tables()

    def table(self, name: str) -> DataFrame:
        """Retrieve a previously registered table by name."""
        return DataFrame(self.ctx.table(name))

    def table_exist(self, name: str) -> bool:
        """Return whether a table with the given name exists."""
        return self.ctx.table_exist(name)

    def empty_table(self) -> DataFrame:
        """Create an empty :py:class:`~datafusion.dataframe.DataFrame`."""
        return DataFrame(self.ctx.empty_table())

    def session_id(self) -> str:
        """Return an id that uniquely identifies this :py:class:`SessionContext`."""
        return self.ctx.session_id()

    def read_json(
        self,
        path: str | pathlib.Path,
        schema: pa.Schema | None = None,
        schema_infer_max_records: int = 1000,
        file_extension: str = ".json",
        table_partition_cols: list[tuple[str, str | pa.DataType]] | None = None,
        file_compression_type: str | None = None,
    ) -> DataFrame:
        """Read a line-delimited JSON data source.

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
        table_partition_cols = self._convert_table_partition_cols(table_partition_cols)
        return DataFrame(
            self.ctx.read_json(
                str(path),
                schema,
                schema_infer_max_records,
                file_extension,
                table_partition_cols,
                file_compression_type,
            )
        )

    def read_csv(
        self,
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
        table_partition_cols = self._convert_table_partition_cols(table_partition_cols)

        path = [str(p) for p in path] if isinstance(path, list) else str(path)

        return DataFrame(
            self.ctx.read_csv(
                path,
                schema,
                has_header,
                delimiter,
                schema_infer_max_records,
                file_extension,
                table_partition_cols,
                file_compression_type,
            )
        )

    def read_parquet(
        self,
        path: str | pathlib.Path,
        table_partition_cols: list[tuple[str, str | pa.DataType]] | None = None,
        parquet_pruning: bool = True,
        file_extension: str = ".parquet",
        skip_metadata: bool = True,
        schema: pa.Schema | None = None,
        file_sort_order: list[list[Expr | SortExpr]] | None = None,
    ) -> DataFrame:
        """Read a Parquet source into a :py:class:`~datafusion.dataframe.Dataframe`.

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
        table_partition_cols = self._convert_table_partition_cols(table_partition_cols)
        file_sort_order = (
            [sort_list_to_raw_sort_list(f) for f in file_sort_order]
            if file_sort_order is not None
            else None
        )
        return DataFrame(
            self.ctx.read_parquet(
                str(path),
                table_partition_cols,
                parquet_pruning,
                file_extension,
                skip_metadata,
                schema,
                file_sort_order,
            )
        )

    def read_avro(
        self,
        path: str | pathlib.Path,
        schema: pa.Schema | None = None,
        file_partition_cols: list[tuple[str, str | pa.DataType]] | None = None,
        file_extension: str = ".avro",
    ) -> DataFrame:
        """Create a :py:class:`DataFrame` for reading Avro data source.

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
        file_partition_cols = self._convert_table_partition_cols(file_partition_cols)
        return DataFrame(
            self.ctx.read_avro(str(path), schema, file_partition_cols, file_extension)
        )

    def read_table(self, table: Table) -> DataFrame:
        """Creates a :py:class:`~datafusion.dataframe.DataFrame` from a table.

        For a :py:class:`~datafusion.catalog.Table` such as a
        :py:class:`~datafusion.catalog.ListingTable`, create a
        :py:class:`~datafusion.dataframe.DataFrame`.
        """
        return DataFrame(self.ctx.read_table(table.table))

    def execute(self, plan: ExecutionPlan, partitions: int) -> RecordBatchStream:
        """Execute the ``plan`` and return the results."""
        return RecordBatchStream(self.ctx.execute(plan._raw_plan, partitions))

    @staticmethod
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
