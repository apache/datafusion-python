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

from ._internal import SessionConfig as SessionConfigInternal
from ._internal import RuntimeConfig as RuntimeConfigInternal
from ._internal import SQLOptions as SQLOptionsInternal
from ._internal import SessionContext as SessionContextInternal
from ._internal import LogicalPlan, ExecutionPlan  # TODO MAKE THIS A DEFINED CLASS

from datafusion._internal import AggregateUDF
from datafusion.catalog import Catalog, Table
from datafusion.dataframe import DataFrame
from datafusion.expr import Expr
from datafusion.record_batch import RecordBatchStream
from datafusion.udf import ScalarUDF

from typing import Any, TYPE_CHECKING
from typing_extensions import deprecated

if TYPE_CHECKING:
    import pyarrow
    import pandas
    import polars


class SessionConfig:
    """Session configuration options."""

    def __init__(self, config_options: dict[str, str] = {}) -> None:
        """Create a new `SessionConfig` with the given configuration options.

        Parameters
        ----------
        config_options : dict[str, str]
            Configuration options.
        """
        self.config_internal = SessionConfigInternal(config_options)

    def with_create_default_catalog_and_schema(
        self, enabled: bool = True
    ) -> SessionConfig:
        """Control whether the default catalog and schema will be automatically created.

        Parameters
        ----------
        enabled : bool
            Whether the default catalog and schema will be automatically created.

        Returns:
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        self.config_internal = (
            self.config_internal.with_create_default_catalog_and_schema(enabled)
        )
        return self

    def with_default_catalog_and_schema(
        self, catalog: str, schema: str
    ) -> SessionConfig:
        """Select a name for the default catalog and shcema.

        Parameters
        ----------
        catalog : str
            Catalog name.
        schema : str
            Schema name.

        Returns:
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_default_catalog_and_schema(
            catalog, schema
        )
        return self

    def with_information_schema(self, enabled: bool = True) -> SessionConfig:
        """Enable or disable the inclusion of `information_schema` virtual tables.

        Parameters
        ----------
        enabled : bool
            Whether to include `information_schema` virtual tables.

        Returns:
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_information_schema(enabled)
        return self

    def with_batch_size(self, batch_size: int) -> SessionConfig:
        """Customize batch size.

        Parameters
        ----------
        batch_size : int
            Batch size.

        Returns:
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_batch_size(batch_size)
        return self

    def with_target_partitions(self, target_partitions: int) -> SessionConfig:
        """Customize the number of target partitions for query execution.

        Increasing partitions can increase concurrency.

        Parameters
        ----------
        target_partitions : int
            Number of target partitions.

        Returns:
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_target_partitions(
            target_partitions
        )
        return self

    def with_repartition_aggregations(self, enabled: bool = True) -> SessionConfig:
        """Enable or disable the use of repartitioning for aggregations.

        Enabling this improves parallelism.

        Parameters
        ----------
        enabled : bool
            Whether to use repartitioning for aggregations.

        Returns:
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_repartition_aggregations(
            enabled
        )
        return self

    def with_repartition_joins(self, enabled: bool = True) -> SessionConfig:
        """Enable or disable the use of repartitioning for joins to improve parallelism.

        Parameters
        ----------
        enabled : bool
            Whether to use repartitioning for joins.

        Returns:
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_repartition_joins(enabled)
        return self

    def with_repartition_windows(self, enabled: bool = True) -> SessionConfig:
        """Enable or disable the use of repartitioning for window functions to improve parallelism.

        Parameters
        ----------
        enabled : bool
            Whether to use repartitioning for window functions.

        Returns:
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_repartition_windows(enabled)
        return self

    def with_repartition_sorts(self, enabled: bool = True) -> SessionConfig:
        """Enable or disable the use of repartitioning for window functions to improve parallelism.

        Parameters
        ----------
        enabled : bool
            Whether to use repartitioning for window functions.

        Returns:
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_repartition_sorts(enabled)
        return self

    def with_repartition_file_scans(self, enabled: bool = True) -> SessionConfig:
        """Enable or disable the use of repartitioning for file scans.

        Parameters
        ----------
        enabled : bool
            Whether to use repartitioning for file scans.

        Returns:
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_repartition_file_scans(enabled)
        return self

    def with_repartition_file_min_size(self, size: int) -> SessionConfig:
        """Set minimum file range size for repartitioning scans.

        Parameters
        ----------
        size : int
            Minimum file range size.

        Returns:
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_repartition_file_min_size(size)
        return self

    def with_parquet_pruning(self, enabled: bool = True) -> SessionConfig:
        """Enable or disable the use of pruning predicate for parquet readers to skip row groups.

        Parameters
        ----------
        enabled : bool
            Whether to use pruning predicate for parquet readers.

        Returns:
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.with_parquet_pruning(enabled)
        return self

    def set(self, key: str, value: str) -> SessionConfig:
        """Set a configuration option.

        Parameters
        ----------
        key : str
            Option key.
        value : str
            Option value.

        Returns:
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        self.config_internal = self.config_internal.set(key, value)
        return self


class RuntimeConfig:
    """Runtime configuration options."""

    def __init__(self) -> None:
        """Create a new `RuntimeConfig` with default values."""
        self.config_internal = RuntimeConfigInternal()

    def with_disk_manager_disabled(self) -> RuntimeConfig:
        """Disable the disk manager, attempts to create temporary files will error.

        Returns:
        -------
        RuntimeConfig
            A new `RuntimeConfig` object with the updated setting.

        Examples:
        --------
        >>> config = RuntimeConfig().with_disk_manager_disabled()
        """
        self.config_internal = self.config_internal.with_disk_manager_disabled()
        return self

    def with_disk_manager_os(self) -> RuntimeConfig:
        """Use the operating system's temporary directory for disk manager.

        Returns:
        -------
        RuntimeConfig
            A new `RuntimeConfig` object with the updated setting.

        Examples:
        --------
        >>> config = RuntimeConfig().with_disk_manager_os()
        """
        self.config_internal = self.config_internal.with_disk_manager_os()
        return self

    def with_disk_manager_specified(self, paths: list[str]) -> RuntimeConfig:
        """Use the specified paths for the disk manager's temporary files.

        Parameters
        ----------
        paths : list[str]
            Paths to use for the disk manager's temporary files.

        Returns:
        -------
        RuntimeConfig
            A new `RuntimeConfig` object with the updated setting.

        Examples:
        --------
        >>> config = RuntimeConfig().with_disk_manager_specified(["/tmp"])
        """
        self.config_internal = self.config_internal.with_disk_manager_specified(paths)
        return self

    def with_unbounded_memory_pool(self) -> RuntimeConfig:
        """Use an unbounded memory pool.

        Returns:
        -------
        RuntimeConfig
            A new `RuntimeConfig` object with the updated setting.

        Examples:
        --------
        >>> config = RuntimeConfig().with_unbounded_memory_pool()
        """
        self.config_internal = self.config_internal.with_unbounded_memory_pool()
        return self

    def with_fair_spill_pool(self, size: int) -> RuntimeConfig:
        """Use a fair spill pool with the specified size.

        This pool works best when you know beforehand the query has multiple spillable
        operators that will likely all need to spill. Sometimes it will cause spills
        even when there was sufficient memory (reserved for other operators) to avoid
        doing so.

        ```text
            ┌───────────────────────z──────────────────────z───────────────┐
            │                       z                      z               │
            │                       z                      z               │
            │       Spillable       z       Unspillable    z     Free      │
            │        Memory         z        Memory        z    Memory     │
            │                       z                      z               │
            │                       z                      z               │
            └───────────────────────z──────────────────────z───────────────┘
        ```

        Parameters
        ----------
        size : int
            Size of the memory pool in bytes.

        Returns:
        -------
        RuntimeConfig
            A new `RuntimeConfig` object with the updated setting.

        Examples:
        --------
        ```python
        >>> config = RuntimeConfig().with_fair_spill_pool(1024)
        ```
        """
        self.config_internal = self.config_internal.with_fair_spill_pool(size)
        return self

    def with_greedy_memory_pool(self, size: int) -> RuntimeConfig:
        """Use a greedy memory pool with the specified size.

        This pool works well for queries that do not need to spill or have a single
        spillable operator. See `RuntimeConfig.with_fair_spill_pool` if there are
        multiple spillable operators that all will spill.

        Parameters
        ----------
        size : int
            Size of the memory pool in bytes.

        Returns:
        -------
        RuntimeConfig
            A new `RuntimeConfig` object with the updated setting.

        Examples:
        --------
        >>> config = RuntimeConfig().with_greedy_memory_pool(1024)
        """
        self.config_internal = self.config_internal.with_greedy_memory_pool(size)
        return self

    def with_temp_file_path(self, path: str) -> RuntimeConfig:
        """Use the specified path to create any needed temporary files.

        Parameters
        ----------
        path : str
            Path to use for temporary files.

        Returns:
        -------
        RuntimeConfig
            A new `RuntimeConfig` object with the updated setting.

        Examples:
        --------
        >>> config = RuntimeConfig().with_temp_file_path("/tmp")
        """
        self.config_internal = self.config_internal.with_temp_file_path(path)
        return self


class SQLOptions:
    """Options to be used when performing SQL queries on the ``SessionContext``."""

    def __init__(self) -> None:
        """Create a new `SQLOptions` with default values.

        The default values are:
        - DDL commands are allowed
        - DML commands are allowed
        - Statements are allowed
        """
        self.options_internal = SQLOptionsInternal()

    def with_allow_ddl(self, allow: bool = True) -> SQLOptions:
        """Should DDL (Data Definition Language) commands be run?

        Examples of DDL commands include `CREATE TABLE` and `DROP TABLE`.

        Parameters
        ----------
        allow : bool
            Allow DDL commands to be run.

        Returns:
        -------
        SQLOptions
            A new `SQLOptions` object with the updated setting.


        Examples:
        --------
        >>> options = SQLOptions().with_allow_ddl(True)
        """
        self.options_internal = self.options_internal.with_allow_ddl(allow)
        return self

    def with_allow_dml(self, allow: bool = True) -> SQLOptions:
        """Should DML (Data Manipulation Language) commands be run?

        Examples of DML commands include `INSERT INTO` and `DELETE`.

        Parameters
        ----------
        allow : bool
            Allow DML commands to be run.

        Returns:
        -------
        SQLOptions
            A new `SQLOptions` object with the updated setting.


        Examples:
        --------
        >>> options = SQLOptions().with_allow_dml(True)
        """
        self.options_internal = self.options_internal.with_allow_dml(allow)
        return self

    def with_allow_statements(self, allow: bool = True) -> SQLOptions:
        """Should statements such as `SET VARIABLE` and `BEGIN TRANSACTION` be run?

        Parameters
        ----------
        allow : bool
            Allow statements to be run.

        Returns:
        -------
        SQLOptions
            A new `SQLOptions` object with the updated setting.

        Examples:
        --------
        >>> options = SQLOptions().with_allow_statements(True)
        """
        self.options_internal = self.options_internal.with_allow_statements(allow)
        return self


class SessionContext:
    """This is the main interface for executing queries and creating DataFrames.

    See https://datafusion.apache.org/python/user-guide/basics.html for additional information.
    """

    def __init__(
        self, config: SessionConfig | None = None, runtime: RuntimeConfig | None = None
    ) -> None:
        """Main interface for executing queries with DataFusion.

        Maintains the state of the connection between a user and an instance
        of the connection between a user and an instance of the DataFusion
        engine.

        Parameters
        ----------
        config : SessionConfig | None
            Session configuration options.
        runtime : RuntimeConfig | None
            Runtime configuration options.

        Examples:
        --------
        The following example demostrates how to use the context to execute
        a query against a CSV data source using the `DataFrame` API:

        ```python
        from datafusion import SessionContext

        ctx = SessionContext()
        df = ctx.read_csv("data.csv")
        ```
        """
        config = config.config_internal if config is not None else None
        runtime = runtime.config_internal if config is not None else None

        self.ctx = SessionContextInternal(config, runtime)

    def register_object_store(self, schema: str, store: Any, host: str | None) -> None:
        """Add a new object store into the session."""
        self.ctx.register_object_store(schema, store, host)

    def register_listing_table(
        self,
        name: str,
        path: str,
        table_partition_cols: list[tuple[str, str]] = [],
        file_extension: str = ".parquet",
        schema: pyarrow.Schema | None = None,
        file_sort_order: list[list[Expr]] | None = None,
    ) -> None:
        """Registers a Table that can assemble multiple files from locations in an ``ObjectStore`` instance into a single table."""
        if file_sort_order is not None:
            file_sort_order = [[x.expr for x in xs] for xs in file_sort_order]
        self.ctx.register_listing_table(
            name, path, table_partition_cols, file_extension, schema, file_sort_order
        )

    def sql(self, query: str) -> DataFrame:
        """Create a `DataFrame` from SQL query text.

        Note: This API implements DDL statements such as `CREATE TABLE` and
        `CREATE VIEW` and DML statements such as `INSERT INTO` with in-memory
        default implementation. See `SessionContext.sql_with_options`.

        Parameters
        ----------
        query : str
            SQL query text.

        Returns:
        -------
        DataFrame
            DataFrame representation of the SQL query.
        """
        return DataFrame(self.ctx.sql(query))

    def sql_with_options(self, query: str, options: SQLOptions) -> DataFrame:
        """Create a `DataFrame` from SQL query text, first validating that the query is allowed by the provided options.

        Parameters
        ----------
        query : str
            SQL query text.
        options : SQLOptions
            SQL options.

        Returns:
        -------
        DataFrame
            DataFrame representation of the SQL query.
        """
        return DataFrame(self.ctx.sql_with_options(query, options.options_internal))

    def create_dataframe(
        self,
        partitions: list[list[pyarrow.RecordBatch]],
        name: str | None = None,
        schema: pyarrow.Schema | None = None,
    ) -> DataFrame:
        """Create and return a dataframe using the provided partitions."""
        return DataFrame(self.ctx.create_dataframe(partitions, name, schema))

    def create_dataframe_from_logical_plan(self, plan: LogicalPlan) -> DataFrame:
        """Create a `DataFrame` from an existing logical plan.

        Parameters
        ----------
        plan : LogicalPlan
            Logical plan.

        Returns:
        -------
        DataFrame
            DataFrame representation of the logical plan.
        """
        return DataFrame(self.ctx.create_dataframe_from_logical_plan(plan))

    def from_pylist(
        self, data: list[dict[str, Any]], name: str | None = None
    ) -> DataFrame:
        """Create a `DataFrame` from a list of dictionaries.

        Parameters
        ----------
        data : list[dict[str, Any]]
            List of dictionaries.
        name : str | None
            Name of the DataFrame.

        Returns:
        -------
        DataFrame
            DataFrame representation of the list of dictionaries.
        """
        return DataFrame(self.ctx.from_pylist(data, name))

    def from_pydict(
        self, data: dict[str, list[Any]], name: str | None = None
    ) -> DataFrame:
        """Create a `DataFrame` from a dictionary of lists.

        Parameters
        ----------
        data : dict[str, list[Any]]
            Dictionary of lists.
        name : str | None
            Name of the DataFrame.

        Returns:
        -------
        DataFrame
            DataFrame representation of the dictionary of lists.
        """
        return DataFrame(self.ctx.from_pydict(data, name))

    def from_arrow_table(
        self, data: pyarrow.Table, name: str | None = None
    ) -> DataFrame:
        """Create a `DataFrame` from an Arrow table.

        Parameters
        ----------
        data : pyarrow.Table
            Arrow table.
        name : str | None
            Name of the DataFrame.

        Returns:
        -------
        DataFrame
            DataFrame representation of the Arrow table.
        """
        return DataFrame(self.ctx.from_arrow_table(data, name))

    def from_pandas(self, data: pandas.DataFrame, name: str | None = None) -> DataFrame:
        """Create a `DataFrame` from a Pandas DataFrame.

        Parameters
        ----------
        data : pandas.DataFrame
            Pandas DataFrame.
        name : str | None
            Name of the DataFrame.

        Returns:
        -------
        DataFrame
            DataFrame representation of the Pandas DataFrame.
        """
        return DataFrame(self.ctx.from_pandas(data, name))

    def from_polars(self, data: polars.DataFrame, name: str | None = None) -> DataFrame:
        """Create a `DataFrame` from a Polars DataFrame.

        Parameters
        ----------
        data : polars.DataFrame
            Polars DataFrame.
        name : str | None
            Name of the DataFrame.

        Returns:
        -------
        DataFrame
            DataFrame representation of the Polars DataFrame.
        """
        return DataFrame(self.ctx.from_polars(data, name))

    def register_table(self, name: str, table: pyarrow.Table) -> None:
        """Register a table with the given name into the session."""
        self.ctx.register_table(name, table)

    def deregister_table(self, name: str) -> None:
        """Remove a table from the session."""
        self.ctx.deregister_table(name)

    def register_record_batches(
        self, name: str, partitions: list[list[pyarrow.RecordBatch]]
    ) -> None:
        """Convert the provided partitions into a table and register it into the session using the given name."""
        self.ctx.register_record_batches(name, partitions)

    def register_parquet(
        self,
        name: str,
        path: str,
        table_partition_cols: list[tuple[str, str]] = [],
        parquet_pruning: bool = True,
        file_extension: str = ".parquet",
        skip_metadata: bool = True,
        schema: pyarrow.Schema | None = None,
        file_sort_order: list[list[Expr]] | None = None,
    ) -> None:
        """Register a Parquet file as a table.

        The registered table can be referenced from SQL statement executed against
        this context.

        Parameters
        ----------
        name : str
            Name of the table to register.
        path : str
            Path to the Parquet file.
        table_partition_cols : list[tuple[str, str]], optional
            Partition columns, by default []
        parquet_pruning : bool, optional
            Whether the parquet reader should use the predicate to prune row groups, by default True
        file_extension : str, optional
            File extension; only files with this extension are selected for data input, by default ".parquet"
        skip_metadata : bool, optional
            Whether the parquet reader should skip any metadata that may be in the file
            schema. This can help avoid schema conflicts due to metadata. by default True
        schema : pyarrow.Schema | None, optional
            The data source schema, by default None
        file_sort_order : list[list[Expr]] | None, optional
            Sort order for the file, by default None
        """
        self.ctx.register_parquet(
            name,
            path,
            table_partition_cols,
            parquet_pruning,
            file_extension,
            skip_metadata,
            schema,
            file_sort_order,
        )

    def register_csv(
        self,
        name: str,
        path: str,
        schema: pyarrow.Schema | None = None,
        has_header: bool = True,
        delimiter: str = ",",
        schema_infer_max_records: int = 1000,
        file_extension: str = ".csv",
        file_compression_type: str | None = None,
    ) -> None:
        """Register a CSV file as a table.

        The registered table can be referenced from SQL statement executed against.

        Parameters
        ----------
        name : str
            Name of the table to register.
        path : str
            Path to the CSV file.
        schema : pyarrow.Schema | None, optional
            An optional schema representing the CSV file. If None, the CSV reader will try to infer it based on data in file, by default None
        has_header : bool, optional
            Whether the CSV file have a header. If schema inference is run on a file with no headers, default column names are created, by default True
        delimiter : str, optional
            An optional column delimiter, by default ","
        schema_infer_max_records : int, optional
            Maximum number of rows to read from CSV files for schema inference if needed, by default 1000
        file_extension : str, optional
            File extension; only files with this extension are selected for data input, by default ".csv"
        file_compression_type : str | None, optional
            File compression type, by default None
        """
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
        path: str,
        schema: pyarrow.Schema | None = None,
        schema_infer_max_records: int = 1000,
        file_extension: str = ".json",
        table_partition_cols: list[tuple[str, str]] = [],
        file_compression_type: str | None = None,
    ) -> None:
        """Register a JSON file as a table.

        The registered table can be referenced from SQL statement executed against
        this context.

        Parameters
        ----------
        name : str
            Name of the table to register.
        path : str
            Path to the JSON file.
        schema : pyarrow.Schema | None, optional
            The data source schema, by default None
        schema_infer_max_records : int, optional
            Maximum number of rows to read from JSON files for schema inference if needed, by default 1000
        file_extension : str, optional
            File extension; only files with this extension are selected for data input, by default ".json"
        table_partition_cols : list[tuple[str, str]], optional
            Partition columns, by default []
        file_compression_type : str | None, optional
            File compression type, by default None
        """
        self.ctx.register_json(
            name,
            path,
            schema,
            schema_infer_max_records,
            file_extension,
            table_partition_cols,
            file_compression_type,
        )

    def register_avro(
        self,
        name: str,
        path: str,
        schema: pyarrow.Schema | None = None,
        file_extension: str = ".avro",
        table_partition_cols: list[tuple[str, str]] = [],
    ) -> None:
        """Register an Avro file as a table.

        The registered table can be referenced from SQL statement executed against
        this context.

        Parameters
        ----------
        name : str
            Name of the table to register.
        path : str
            Path to the Avro file.
        schema : pyarrow.Schema | None, optional
            The data source schema, by default None
        file_extension : str, optional
            File extension to select, by default ".avro"
        table_partition_cols : list[tuple[str, str]], optional
            Partition columns, by default []
        """
        self.ctx.register_avro(name, path, schema, file_extension, table_partition_cols)

    def register_dataset(self, name: str, dataset: pyarrow.dataset.Dataset) -> None:
        """Register a `pyarrow.dataset.Dataset` as a table.

        Parameters
        ----------
        name : str
            Name of the table to register.
        dataset : dataset.Dataset
            PyArrow dataset.
        """
        self.ctx.register_dataset(name, dataset)

    def register_udf(self, udf: ScalarUDF) -> None:
        """Register a user-defined function (UDF) with the context.

        Parameters
        ----------
        udf : ScalarUDF
            User-defined function.
        """
        self.ctx.register_udf(udf.udf)

    def register_udaf(self, udaf: AggregateUDF) -> None:
        """Register a user-defined aggregation function (UDAF) with the context.

        Parameters
        ----------
        udaf : AggregateUDF
            User-defined aggregation function.
        """
        self.ctx.register_udaf(udaf)

    def catalog(self, name: str = "datafusion") -> Catalog:
        """Retrieve a catalog by name.

        Parameters
        ----------
        name : str, optional
            Name of the catalog to retrieve, by default "datafusion".

        Returns:
        -------
        Catalog
            Catalog representation.
        """
        return self.ctx.catalog(name)

    @deprecated(
        "Use the catalog provider interface `SessionContext.catalog` to "
        "examine available catalogs, schemas and tables"
    )
    def tables(self) -> set[str]:
        """Deprecated."""
        return self.ctx.tables()

    def table(self, name: str) -> DataFrame:
        """Retrieve a `DataFrame` representing a previously registered table.

        Parameters
        ----------
        name : str
            Name of the table to retrieve.

        Returns:
        -------
        DataFrame
            DataFrame representation of the table.
        """
        return DataFrame(self.ctx.table(name))

    def table_exist(self, name: str) -> bool:
        """Return whether a table with the given name exists.

        Parameters
        ----------
        name : str
            Name of the table to check.

        Returns:
        -------
        bool
            Whether a table with the given name exists.
        """
        return self.ctx.table_exist(name)

    def empty_table(self) -> DataFrame:
        """Create an empty `DataFrame`.

        Returns:
        -------
        DataFrame
            An empty DataFrame.
        """
        return DataFrame(self.ctx.empty_table())

    def session_id(self) -> str:
        """Retrun an id that uniquely identifies this `SessionContext`.

        Returns:
        -------
        str
            Unique session identifier
        """
        return self.ctx.session_id()

    def read_json(
        self,
        path: str,
        schema: pyarrow.Schema | None = None,
        schema_infer_max_records: int = 1000,
        file_extension: str = ".json",
        table_partition_cols: list[tuple[str, str]] = [],
        file_compression_type: str | None = None,
    ) -> DataFrame:
        """Create a `DataFrame` for reading a line-delimited JSON data source.

        Parameters
        ----------
        path : str
            Path to the JSON file
        schema : pyarrow.Schema | None, optional
            The data source schema, by default None
        schema_infer_max_records : int, optional
            Maximum number of rows to read from JSON files for schema inference if needed, by default 1000
        file_extension : str, optional
            File extension; only files with this extension are selected for data input, by default ".json"
        table_partition_cols : list[tuple[str, str]], optional
            Partition columns, by default []
        file_compression_type : str | None, optional
            File compression type, by default None

        Returns:
        -------
        DataFrame
            DataFrame representation of the read JSON files
        """
        return DataFrame(
            self.ctx.read_json(
                path,
                schema,
                schema_infer_max_records,
                file_extension,
                table_partition_cols,
                file_compression_type,
            )
        )

    def read_csv(
        self,
        path: str,
        schema: pyarrow.Schema | None = None,
        has_header: bool = True,
        delimiter: str = ",",
        schema_infer_max_records: int = 1000,
        file_extension: str = ".csv",
        table_partition_cols: list[tuple[str, str]] = [],
        file_compression_type: str | None = None,
    ) -> DataFrame:
        """Create a `DataFrame` for reading a CSV data source.

        Parameters
        ----------
        path : str
            Path to the CSV file
        schema : pyarrow.Schema | None, optional
            An optional schema representing the CSV files. If None, the CSV reader will try to infer it based on data in file, by default None
        has_header : bool, optional
            Whether the CSV file have a header. If schema inference is run on a file with no headers, default column names are created, by default True
        delimiter : str, optional
            An optional column delimiter, by default ","
        schema_infer_max_records : int, optional
            Maximum number of rows to read from CSV files for schema inference if needed, by default 1000
        file_extension : str, optional
            File extension; only files with this extension are selected for data input, by default ".csv"
        table_partition_cols : list[tuple[str, str]], optional
            Partition columns, by default []
        file_compression_type : str | None, optional
            File compression type, by default None

        Returns:
        -------
        DataFrame
            DataFrame representation of the read CSV files
        """
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
        path: str,
        table_partition_cols: list[tuple[str, str]] = [],
        parquet_pruning: bool = True,
        file_extension: str = ".parquet",
        skip_metadata: bool = True,
        schema: pyarrow.Schema | None = None,
        file_sort_order: list[list[Expr]] | None = None,
    ) -> DataFrame:
        """Create a `DataFrame` for reading Parquet data source.

        Parameters
        ----------
        path: str
            Path to the Parquet file
        table_partition_cols : list[tuple[str, str]], optional
            Partition columns, by default []
        parquet_pruning : bool, optional
            Whether the parquet reader should use the predicate to prune row groups, by default True
        file_extension : str, optional
            File extension; only files with this extension are selected for data input, by default ".parquet"
        skip_metadata : bool, optional
            Whether the parquet reader should skip any metadata that may be in the file
            schema. This can help avoid schema conflicts due to metadata. by default True
        schema : pyarrow.Schema | None, optional
            An optional schema representing the parquet files. If None, the parquet
            reader will try to infer it based on data in the file, by default None
        file_sort_order : list[list[Expr]] | None, optional
            Sort order for the file, by default None

        Returns:
        -------
        DataFrame
            DataFrame representation of the read Parquet files
        """
        return DataFrame(
            self.ctx.read_parquet(
                path,
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
        path: str,
        schema: pyarrow.Schema | None = None,
        file_partition_cols: list[tuple[str, str]] = [],
        file_extension: str = ".avro",
    ) -> DataFrame:
        """Create a `DataFrame` for reading Avro data source.

        Parameters
        ----------
        path : str
            Path to the Avro file
        schema : pyarrow.Schema | None, optional
            The data source schema, by default None
        file_partition_cols : list[tuple[str, str]], optional
            Partition columns, by default []
        file_extension : str, optional
            File extension to select, by default ".avro"

        Returns:
        -------
        DataFrame
            DataFrame representation of the read Avro file
        """
        return DataFrame(
            self.ctx.read_avro(path, schema, file_partition_cols, file_extension)
        )

    def read_table(self, table: Table) -> DataFrame:
        """Creates a ``DataFrame`` for a ``Table`` such as a ``ListingTable``."""
        return DataFrame(self.ctx.read_table(table))

    def execute(self, plan: ExecutionPlan, partitions: int) -> RecordBatchStream:
        """Execute the `plan` and return the results."""
        return RecordBatchStream(self.ctx.execute(plan, partitions))
