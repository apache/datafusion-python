from __future__ import annotations

from typing import Any
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    import pyarrow as pa
    import pandas as pd
    import polars as pl  # type: ignore [import-not-found]

    from pyarrow import dataset
    from typing_extensions import deprecated

    from . import DataFrame, Expr, ScalarUDF, AggregateUDF
    from ._internal import ExecutionPlan, RecordBatchStream, LogicalPlan, Catalog, Table

class SessionConfig:
    def __init__(self, config_options: dict[str, str]) -> None:
        """Create a new `SessionConfig` with the given configuration options.

        Parameters
        ----------
        config_options : dict[str, str]
            Configuration options.
        """
        ...

    def with_create_default_catalog_and_schema(self, enabled: bool) -> SessionConfig:
        """Control whether the default catalog and schema will be automatically created.

        Parameters
        ----------
        enabled : bool
            Whether the default catalog and schema will be automatically created.

        Returns
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        ...

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

        Returns
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        ...

    def with_information_schema(self, enabled: bool) -> SessionConfig:
        """Enable or disable the inclusion of `information_schema` virtual tables.

        Parameters
        ----------
        enabled : bool
            Whether to include `information_schema` virtual tables.

        Returns
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        ...

    def with_batch_size(self, batch_size: int) -> SessionConfig:
        """Customize batch size.

        Parameters
        ----------
        batch_size : int
            Batch size.

        Returns
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        ...

    def with_target_partitions(self, target_partitions: int) -> SessionConfig:
        """Customize the number of target partitions for query execution.

        Increasing partitions can increase concurrency.

        Parameters
        ----------
        target_partitions : int
            Number of target partitions.

        Returns
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        ...

    def with_repartition_aggregations(self, enabled: bool) -> SessionConfig:
        """Enable or disable the use of repartitioning for aggregations.

        Enabling this improves parallelism.

        Parameters
        ----------
        enabled : bool
            Whether to use repartitioning for aggregations.

        Returns
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        ...

    def with_repartition_joins(self, enabled: bool) -> SessionConfig:
        """Enable or disable the use of repartitioning for joins to improve parallelism.

        Parameters
        ----------
        enabled : bool
            Whether to use repartitioning for joins.

        Returns
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        ...

    def with_repartition_windows(self, enabled: bool) -> SessionConfig:
        """Enable or disable the use of repartitioning for window functions to improve parallelism.

        Parameters
        ----------
        enabled : bool
            Whether to use repartitioning for window functions.

        Returns
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        ...

    def with_repartition_sorts(self, enabled: bool) -> SessionConfig:
        """Enable or disable the use of repartitioning for window functions to improve parallelism.

        Parameters
        ----------
        enabled : bool
            Whether to use repartitioning for window functions.

        Returns
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        ...

    def with_repartition_file_scans(self, enabled: bool) -> SessionConfig:
        """Enable or disable the use of repartitioning for file scans.

        Parameters
        ----------
        enabled : bool
            Whether to use repartitioning for file scans.

        Returns
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        ...

    def with_repartition_file_min_size(self, size: int) -> SessionConfig:
        """Set minimum file range size for repartitioning scans.

        Parameters
        ----------
        size : int
            Minimum file range size.

        Returns
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        ...

    def with_parquet_pruning(self, enabled: bool) -> SessionConfig:
        """Enable or disable the use of pruning predicate for parquet readers to skip row groups.

        Parameters
        ----------
        enabled : bool
            Whether to use pruning predicate for parquet readers.

        Returns
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        ...

    def set(self, key: str, value: str) -> SessionConfig:
        """Set a configuration option.

        Parameters
        ----------
        key : str
            Option key.
        value : str
            Option value.

        Returns
        -------
        SessionConfig
            A new `SessionConfig` object with the updated setting.
        """
        ...

class RuntimeConfig:
    def __init__(self) -> None:
        """Create a new `RuntimeConfig` with default values."""
        ...

    def with_disk_manager_disabled(self) -> RuntimeConfig:
        """Disable the disk manager, attempts to create temporary files will error.

        Returns
        -------
        RuntimeConfig
            A new `RuntimeConfig` object with the updated setting.

        Examples
        --------
        >>> config = RuntimeConfig().with_disk_manager_disabled()
        """
        ...

    def with_disk_manager_os(self) -> RuntimeConfig:
        """Use the operating system's temporary directory for disk manager.

        Returns
        -------
        RuntimeConfig
            A new `RuntimeConfig` object with the updated setting.

        Examples
        --------
        >>> config = RuntimeConfig().with_disk_manager_os()
        """
        ...

    def with_disk_manager_specified(self, paths: list[str]) -> RuntimeConfig:
        """Use the specified paths for the disk manager's temporary files.

        Parameters
        ----------
        paths : list[str]
            Paths to use for the disk manager's temporary files.

        Returns
        -------
        RuntimeConfig
            A new `RuntimeConfig` object with the updated setting.

        Examples
        --------
        >>> config = RuntimeConfig().with_disk_manager_specified(["/tmp"])
        """
        ...

    def with_unbounded_memory_pool(self) -> RuntimeConfig:
        """Use an unbounded memory pool.

        Returns
        -------
        RuntimeConfig
            A new `RuntimeConfig` object with the updated setting.

        Examples
        --------
        >>> config = RuntimeConfig().with_unbounded_memory_pool()
        """
        ...

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

        Returns
        -------
        RuntimeConfig
            A new `RuntimeConfig` object with the updated setting.

        Examples
        --------
        ```python
        >>> config = RuntimeConfig().with_fair_spill_pool(1024)
        ```
        """
        ...

    def with_greedy_memory_pool(self, size: int) -> RuntimeConfig:
        """Use a greedy memory pool with the specified size.

        This pool works well for queries that do not need to spill or have a single
        spillable operator. See `RuntimeConfig.with_fair_spill_pool` if there are
        multiple spillable operators that all will spill.

        Parameters
        ----------
        size : int
            Size of the memory pool in bytes.

        Returns
        -------
        RuntimeConfig
            A new `RuntimeConfig` object with the updated setting.

        Examples
        --------
        >>> config = RuntimeConfig().with_greedy_memory_pool(1024)
        """
        ...

    def with_temp_file_path(self, path: str) -> RuntimeConfig:
        """Use the specified path to create any needed temporary files.

        Parameters
        ----------
        path : str
            Path to use for temporary files.

        Returns
        -------
        RuntimeConfig
            A new `RuntimeConfig` object with the updated setting.

        Examples
        --------
        >>> config = RuntimeConfig().with_temp_file_path("/tmp")
        """
        ...

class SQLOptions:
    def __init__(self) -> None:
        """Create a new `SQLOptions` with default values.

        The default values are:
        - DDL commands are allowed
        - DML commands are allowed
        - Statements are allowed
        """
        ...

    def with_allow_ddl(self, allow: bool) -> SQLOptions:
        """Should DDL (Data Definition Language) commands be run?

        Examples of DDL commands include `CREATE TABLE` and `DROP TABLE`.

        Parameters
        ----------
        allow : bool
            Allow DDL commands to be run.

        Returns
        -------
        SQLOptions
            A new `SQLOptions` object with the updated setting.


        Examples
        --------
        >>> options = SQLOptions().with_allow_ddl(True)
        """
        ...

    def with_allow_dml(self, allow: bool) -> SQLOptions:
        """Should DML (Data Manipulation Language) commands be run?

        Examples of DML commands include `INSERT INTO` and `DELETE`.

        Parameters
        ----------
        allow : bool
            Allow DML commands to be run.

        Returns
        -------
        SQLOptions
            A new `SQLOptions` object with the updated setting.


        Examples
        --------
        >>> options = SQLOptions().with_allow_dml(True)
        """
        ...

    def with_allow_statements(self, allow: bool) -> SQLOptions:
        """Should statements such as `SET VARIABLE` and `BEGIN TRANSACTION` be run?

        Parameters
        ----------
        allow : bool
            Allow statements to be run.

        Returns
        -------
        SQLOptions
            A new `SQLOptions` object with the updated setting.

        Examples
        --------
        >>> options = SQLOptions().with_allow_statements(True)
        """
        ...

class SessionContext:
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

        Examples
        --------
        The following example demostrates how to use the context to execute
        a query against a CSV data source using the `DataFrame` API:

        ```python
        from datafusion import SessionContext

        ctx = SessionContext()
        df = ctx.read_csv("data.csv")
        ```
        """
        ...

    def register_object_store(
        self, scheme: str, store: Any, host: str | None
    ) -> None: ...
    def register_listing_table(
        self,
        name: str,
        path: str,
        table_partition_cols: list[tuple[str, str]] = ...,
        file_extension: str = ".parquet",
        schema: pa.Schema | None = None,
        file_sort_order: list[list[Expr]] | None = None,
    ) -> None: ...
    def sql(self, query: str) -> "DataFrame":
        """Create a `DataFrame` from SQL query text.

        Note: This API implements DDL statements such as `CREATE TABLE` and
        `CREATE VIEW` and DML statements such as `INSERT INTO` with in-memory
        default implementation. See `SessionContext.sql_with_options`.

        Parameters
        ----------
        query : str
            SQL query text.

        Returns
        -------
        DataFrame
            DataFrame representation of the SQL query.
        """
        ...

    def sql_with_options(self, query: str, options: SQLOptions) -> "DataFrame":
        """Create a `DataFrame` from SQL query text, first validating that
        the query is allowed by the provided options.

        Parameters
        ----------
        query : str
            SQL query text.
        options : SQLOptions
            SQL options.

        Returns
        -------
        DataFrame
            DataFrame representation of the SQL query.
        """
        ...

    def create_dataframe(
        self,
        partitions: list[list[pa.RecordBatch]],
        name: str | None,
        schema: pa.Schema | None,
    ) -> "DataFrame": ...
    def create_dataframe_from_logical_plan(self, plan: LogicalPlan) -> "DataFrame":
        """Create a `DataFrame` from an existing logical plan.

        Parameters
        ----------
        plan : LogicalPlan
            Logical plan.

        Returns
        -------
        DataFrame
            DataFrame representation of the logical plan.
        """
        ...

    def from_pylist(self, data: list[dict[str, Any]], name: str | None) -> "DataFrame":
        """Create a `DataFrame` from a list of dictionaries.

        Parameters
        ----------
        data : list[dict[str, Any]]
            List of dictionaries.
        name : str | None
            Name of the DataFrame.

        Returns
        -------
        DataFrame
            DataFrame representation of the list of dictionaries.
        """
        ...

    def from_pydict(self, data: dict[str, list[Any]], name: str | None) -> "DataFrame":
        """Create a `DataFrame` from a dictionary of lists.

        Parameters
        ----------
        data : dict[str, list[Any]]
            Dictionary of lists.
        name : str | None
            Name of the DataFrame.

        Returns
        -------
        DataFrame
            DataFrame representation of the dictionary of lists.
        """
        ...

    def from_arrow_table(self, data: pa.Table, name: str | None) -> "DataFrame":
        """Create a `DataFrame` from an Arrow table.

        Parameters
        ----------
        data : pa.Table
            Arrow table.
        name : str | None
            Name of the DataFrame.

        Returns
        -------
        DataFrame
            DataFrame representation of the Arrow table.
        """
        ...

    def from_pandas(self, data: pd.DataFrame, name: str | None) -> "DataFrame":
        """Create a `DataFrame` from a Pandas DataFrame.

        Parameters
        ----------
        data : pd.DataFrame
            Pandas DataFrame.
        name : str | None
            Name of the DataFrame.

        Returns
        -------
        DataFrame
            DataFrame representation of the Pandas DataFrame.
        """
        ...

    def from_polars(self, data: pl.DataFrame, name: str | None) -> "DataFrame":
        """Create a `DataFrame` from a Polars DataFrame.

        Parameters
        ----------
        data : pl.DataFrame
            Polars DataFrame.
        name : str | None
            Name of the DataFrame.

        Returns
        -------
        DataFrame
            DataFrame representation of the Polars DataFrame.
        """
        ...

    def register_table(self, name: str, table: pa.Table) -> None: ...
    def deregister_table(self, name: str) -> None: ...
    def register_record_batches(
        self, name: str, partitions: list[list[pa.RecordBatch]]
    ) -> None: ...
    def register_parquet(
        self,
        name: str,
        path: str,
        table_partition_cols: list[tuple[str, str]] = ...,
        parquet_pruning: bool = True,
        file_extension: str = ".parquet",
        skip_metadata: bool = True,
        schema: pa.Schema | None = None,
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
            Partition columns, by default ...
        parquet_pruning : bool, optional
            Whether the parquet reader should use the predicate to prune row groups, by default True
        file_extension : str, optional
            File extension; only files with this extension are selected for data input, by default ".parquet"
        skip_metadata : bool, optional
            Whether the parquet reader should skip any metadata that may be in the file
            schema. This can help avoid schema conflicts due to metadata. by default True
        schema : pa.Schema | None, optional
            The data source schema, by default None
        file_sort_order : list[list[Expr]] | None, optional
            Sort order for the file, by default None
        """
        ...

    def register_csv(
        self,
        name: str,
        path: str,
        schema: pa.Schema | None = None,
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
        schema : pa.Schema | None, optional
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
        ...

    def register_json(
        self,
        name: str,
        path: str,
        schema: pa.Schema | None = None,
        schema_infer_max_records: int = 1000,
        file_extension: str = ".json",
        table_partition_cols: list[tuple[str, str]] = ...,
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
        schema : pa.Schema | None, optional
            The data source schema, by default None
        schema_infer_max_records : int, optional
            Maximum number of rows to read from JSON files for schema inference if needed, by default 1000
        file_extension : str, optional
            File extension; only files with this extension are selected for data input, by default ".json"
        table_partition_cols : list[tuple[str, str]], optional
            Partition columns, by default ...
        file_compression_type : str | None, optional
            File compression type, by default None
        """
        ...

    def register_avro(
        self,
        name: str,
        path: str,
        schema: pa.Schema | None = None,
        file_extension: str = ".avro",
        table_partition_cols: list[tuple[str, str]] = ...,
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
        schema : pa.Schema | None, optional
            The data source schema, by default None
        file_extension : str, optional
            File extension to select, by default ".avro"
        table_partition_cols : list[tuple[str, str]], optional
            Partition columns, by default ...
        """
        ...

    def register_dataset(self, name: str, dataset: dataset.Dataset) -> None:
        """Register a `pyarrow.dataset.Dataset` as a table.

        Parameters
        ----------
        name : str
            Name of the table to register.
        dataset : dataset.Dataset
            PyArrow dataset.
        """
        ...

    def register_udf(self, udf: ScalarUDF) -> None:
        """Register a user-defined function (UDF) with the context.

        Parameters
        ----------
        udf : ScalarUDF
            User-defined function.
        """
        ...

    def register_udaf(self, udaf: AggregateUDF) -> None:
        """Register a user-defined aggregation function (UDAF) with the context.

        Parameters
        ----------
        udaf : AggregateUDF
            User-defined aggregation function.
        """
        ...

    def catalog(self, name: str = "datafusion") -> Catalog:
        """Retrieve a catalog by name.

        Parameters
        ----------
        name : str, optional
            Name of the catalog to retrieve, by default "datafusion".

        Returns
        -------
        Catalog
            Catalog representation.
        """
        ...

    @deprecated(
        "Use the catalog provider interface `SessionContext.catalog` to "
        "examine available catalogs, schemas and tables"
    )
    def tables(self) -> set[str]: ...
    def table(self, name: str) -> "DataFrame":
        """Retrieve a `DataFrame` representing a previously registered table.

        Parameters
        ----------
        name : str
            Name of the table to retrieve.

        Returns
        -------
        DataFrame
            DataFrame representation of the table.
        """
        ...

    def table_exist(self, name: str) -> bool:
        """Return whether a table with the given name exists.

        Parameters
        ----------
        name : str
            Name of the table to check.

        Returns
        -------
        bool
            Whether a table with the given name exists.
        """
        ...

    def empty_table(self) -> "DataFrame":
        """Create an empty `DataFrame`.

        Returns
        -------
        DataFrame
            An empty DataFrame.
        """
        ...

    def session_id(self) -> str:
        """Retrun an id that uniquely identifies this `SessionContext`.

        Returns
        -------
        str
            Unique session identifier
        """
        ...

    def read_json(
        self,
        path: str,
        schema: pa.Schema | None = None,
        schema_infer_max_records: int = 1000,
        file_extension: str = ".json",
        table_partition_cols: list[tuple[str, str]] = ...,
        file_compression_type: str | None = None,
    ) -> "DataFrame":
        """Create a `DataFrame` for reading a line-delimited JSON data source.

        Parameters
        ----------
        path : str
            Path to the JSON file
        schema : pa.Schema | None, optional
            The data source schema, by default None
        schema_infer_max_records : int, optional
            Maximum number of rows to read from JSON files for schema inference if needed, by default 1000
        file_extension : str, optional
            File extension; only files with this extension are selected for data input, by default ".json"
        table_partition_cols : list[tuple[str, str]], optional
            Partition columns, by default ...
        file_compression_type : str | None, optional
            File compression type, by default None

        Returns
        -------
        DataFrame
            DataFrame representation of the read JSON files
        """
        ...

    def read_csv(
        self,
        path: str,
        schema: pa.Schema | None = None,
        has_header: bool = True,
        delimiter: str = ",",
        schema_infer_max_records: int = 1000,
        file_extension: str = ".csv",
        table_partition_cols: list[tuple[str, str]] = ...,
        file_compression_type: str | None = None,
    ) -> "DataFrame":
        """Create a `DataFrame` for reading a CSV data source.

        Parameters
        ----------
        path : str
            Path to the CSV file
        schema : pa.Schema | None, optional
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
            Partition columns, by default ...
        file_compression_type : str | None, optional
            File compression type, by default None

        Returns
        -------
        DataFrame
            DataFrame representation of the read CSV files
        """
        ...

    def read_parquet(
        self,
        path: str,
        table_partition_cols: list[tuple[str, str]] = [],
        parquet_pruning: bool = True,
        file_extension: str = ".parquet",
        skip_metadata: bool = True,
        schema: pa.Schema | None = None,
        file_sort_order: list[list[Expr]] | None = None,
    ) -> DataFrame:
        """Create a `DataFrame` for reading Parquet data source.

        Parameters
        ----------
        path: str
            Path to the Parquet file
        table_partition_cols : list[tuple[str, str]], optional
            Partition columns, by default ...
        parquet_pruning : bool, optional
            Whether the parquet reader should use the predicate to prune row groups, by default True
        file_extension : str, optional
            File extension; only files with this extension are selected for data input, by default ".parquet"
        skip_metadata : bool, optional
            Whether the parquet reader should skip any metadata that may be in the file
            schema. This can help avoid schema conflicts due to metadata. by default True
        schema : pa.Schema | None, optional
            An optional schema representing the parquet files. If None, the parquet
            reader will try to infer it based on data in the file, by default None
        file_sort_order : list[list[Expr]] | None, optional
            Sort order for the file, by default None

        Returns
        -------
        DataFrame
            DataFrame representation of the read Parquet files
        """
        ...

    def read_avro(
        self,
        path: str,
        schema: pa.Schema | None = None,
        file_partition_cols: list[tuple[str, str]] = ...,
        file_extension: str = ".avro",
    ) -> "DataFrame":
        """Create a `DataFrame` for reading Avro data source.

        Parameters
        ----------
        path : str
            Path to the Avro file
        schema : pa.Schema | None, optional
            The data source schema, by default None
        file_partition_cols : list[tuple[str, str]], optional
            Partition columns, by default ...
        file_extension : str, optional
            File extension to select, by default ".avro"

        Returns
        -------
        DataFrame
            DataFrame representation of the read Avro file
        """
        ...

    def read_table(self, table: Table) -> "DataFrame": ...
    def __repr__(self) -> str: ...
    def execute(self, plan: ExecutionPlan, part: int) -> RecordBatchStream: ...
