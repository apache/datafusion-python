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

from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import pyarrow as pa
from pyarrow.dataset import Dataset
import pandas as pd
import polars as pl
from ..udf import Accumulator, WindowEvaluator
from ..context import ArrowStreamExportable, TableProviderExportable
from .expr import SortExpr, Expr

class Catalog:
    def names(self) -> List[str]: ...

    def database(self, name: str = "public") -> Database: ...


class Database:
    def names(self) -> Set[str]: ...

    def table(self, name: str) -> Table: ...

class Table:
    @property
    def schema(self) -> pa.Schema:
        ...

    @property
    def kind(self) -> str:
        ...

class SessionConfig:
    def __init__(self, config_options: Optional[Dict[str, str]] = None) -> None: ...

    def with_create_default_catalog_and_schema(self, enabled: bool) -> SessionConfig: ...

    def with_default_catalog_and_schema(self, catalog: str, schema: str) -> SessionConfig: ...

    def with_information_schema(self, enabled: bool) -> SessionConfig: ...

    def with_batch_size(self, batch_size: int) -> SessionConfig: ...

    def with_target_partitions(self, target_partitions: int) -> SessionConfig: ...

    def with_repartition_aggregations(self, enabled: bool) -> SessionConfig: ...

    def with_repartition_joins(self, enabled: bool) -> SessionConfig: ...

    def with_repartition_windows(self, enabled: bool) -> SessionConfig: ...

    def with_repartition_sorts(self, enabled: bool) -> SessionConfig: ...

    def with_repartition_file_scans(self, enabled: bool) -> SessionConfig: ...

    def with_repartition_file_min_size(self, size: int) -> SessionConfig: ...

    def with_parquet_pruning(self, enabled: bool) -> SessionConfig: ...

    def set(self, key: str, value: str) -> SessionConfig: ...


class RuntimeEnvBuilder:
    def __init__(self) -> None: ...

    def with_disk_manager_disabled(self) -> RuntimeEnvBuilder: ...

    def with_disk_manager_os(self) -> RuntimeEnvBuilder: ...

    def with_disk_manager_specified(self, paths: List[str]) -> RuntimeEnvBuilder: ...

    def with_unbounded_memory_pool(self) -> RuntimeEnvBuilder: ...

    def with_fair_spill_pool(self, size: int) -> RuntimeEnvBuilder: ...

    def with_greedy_memory_pool(self, size: int) -> RuntimeEnvBuilder: ...

    def with_temp_file_path(self, path: str) -> RuntimeEnvBuilder: ...


class SQLOptions:
    def __init__(self) -> None: ...

    def with_allow_ddl(self, allow: bool) -> SQLOptions: ...

    def with_allow_dml(self, allow: bool) -> SQLOptions: ...

    def with_allow_statements(self, allow: bool) -> SQLOptions: ...


class SessionContext:
    def __init__(self, config: Optional[SessionConfig] = None, runtime: Optional[RuntimeEnvBuilder] = None) -> None: ...

    def enable_url_table(self) -> SessionContext: ...

    def register_object_store(self, schema: str, storage: Any, host: Optional[str] = None): ...

    def register_listing_table(self, name: str, path: str, table_partition_cols: List[Tuple[str, str]] = [], file_extension: str = ".parquet", schema: Optional[pa.Schema] = None, file_sort_order: Optional[List[List[SortExpr]]] = None): ...

    def sql(self, query: str) -> DataFrame: ...

    def sql_with_options(self, query: str, options: Optional[SQLOptions] = None) -> DataFrame: ...

    def create_dataframe(self, partitions: List[List[pa.RecordBatch]], name: Optional[str] = None, schema: Optional[pa.Schema] = None) -> DataFrame: ...

    def create_dataframe_from_logical_plan(self, plan: LogicalPlan) -> DataFrame: ...

    def from_pylist(self, data: list, name: Optional[str] = None) -> DataFrame: ...

    def from_pydict(self, data: dict, name: Optional[str] = None) -> DataFrame: ...

    def from_arrow(self, data: ArrowStreamExportable | pa.RecordBatchReader, name: Optional[str] = None) -> DataFrame: ...

    def from_pandas(self, data: pd.DataFrame, name: Optional[str] = None) -> DataFrame: ...

    def from_polars(self, data: pl.DataFrame, name: Optional[str] = None) -> DataFrame: ...

    def register_table(self, data: str, table: Table): ...

    def deregister_table(self, name: str): ...

    def register_table_provider(self, name: str, provider: TableProviderExportable): ...

    def register_record_batches(self, name: str, partitions: List[List[pa.RecordBatch]]): ...

    def register_parquet(
        self,
        name: str,
        path: str,
        table_partition_cols: List[Tuple[str, str]] = [],
        parquet_pruning: bool = True,
        file_extension: str = ".parquet",
        skip_metadata: bool = True,
        schema: Optional[pa.Schema] = None,
        file_sort_order: Optional[List[List[SortExpr]]] = None,
        **kwargs
        ):
        ...

    def register_csv(
        self,
        name: str,
        path: str | List[str],
        schema: Optional[pa.Schema] = None,
        has_header: bool = True,
        delimiter: str = ",",
        schema_infer_max_records: int = 1000,
        file_extension: str = ".csv",
        file_compression_type: Optional[str] = None,
        **kwargs
        ):
        ...

    def register_json(
        self,
        name: str,
        path: str | Path,
        schema: Optional[pa.Schema] = None,
        schema_infer_max_records: int = 1000,
        file_extension: str = ".json",
        table_partition_cols: List[Tuple[str, str]] = [],
        file_compression_type: Optional[str] = None,
        **kwargs
    ):
        ...

    def register_avro(
        self,
        name: str,
        path: str | Path,
        schema: Optional[pa.Schema] = None,
        file_extension: str = ".avro",
        table_partition_cols: List[Tuple[str, str]] = [],
    ):
        ...

    def register_dataset(
        self,
        name: str,
        dataset: Dataset
    ):
        ...

    def register_udf(self, udf: ScalarUDF): ...

    def register_udaf(self, udaf: AggregateUDF): ...

    def register_udwf(self, udwf: WindowUDF): ...

    def catalog(self, name: str = "datafusion") -> Catalog: ...

    def tables(self) -> Set[str]: ...

    def table(self, name: str) -> DataFrame: ...

    def table_exist(self, name: str) -> bool: ...

    def empty_table(self) -> DataFrame: ...

    def session_id(self) -> str: ...

    def read_json(
        self,
        path: str | List[str],
        schema: Optional[pa.Schema] = None,
        schema_infer_max_records: int = 1000,
        file_extension: str = ".json",
        table_partition_cols: List[Tuple[str, str]] = [],
        file_compression_type: Optional[str] = None,
        **kwargs
        ):
        ...

    def read_csv(
        self,
        path: str | List[str],
        schema: Optional[pa.Schema] = None,
        head_header: bool = True,
        delimiter: str = ",",
        schema_infer_max_records: int = 1000,
        file_extension: str = ".csv",
        table_partition_cols: List[Tuple[str, str]] = [],
        file_compression_type: Optional[str] = None,
        **kwargs
        ):
        ...

    def read_parquet(
        self,
        path: str | List[str],
        table_partition_cols: List[Tuple[str, str]] = [],
        parquet_pruning: bool = True,
        file_extension: str = ".parquet",
        skip_metadata: bool = True,
        schema: Optional[pa.Schema] = None,
        file_sort_order: Optional[List[List[SortExpr]]] = None,
        **kwargs
        ):
        ...

    def read_avro(
        self,
        path: str,
        schema: Optional[pa.Schema] = None,
        table_partition_cols: List[Tuple[str, str]] = [],
        file_extension: str = ".avro",
        **kwargs
        ):
        ...

    def read_table(self, table: Table) -> DataFrame: ...

    def execute(self, plan: ExecutionPlan, part: int) -> RecordBatchStream:
        ...


class DataFrame:
    def __getitem__(self, key: str | List[str] | Tuple[str, ...]) -> DataFrame: ...

    def _repr_html_(self) -> str: ...

    def describe(self) -> DataFrame: ...

    def schema(self) -> pa.Schema: ...

    def select_columns(self, *args: str) -> DataFrame: ...

    def select(self, *args: Expr) -> DataFrame: ...

    def drop(self, *args: str) -> DataFrame: ...

    def filter(self, predicate: Expr) -> DataFrame: ...

    def with_column(self, name: str, expr: Expr) -> DataFrame: ...

    def with_columns(self, exprs: List[Expr]) -> DataFrame: ...

    def with_column_renamed(self, old_name: str, new_name: str) -> DataFrame: ...

    def aggregate(self, group_by: List[Expr], aggs: List[Expr]) -> DataFrame: ...

    def sort(self, *exprs: SortExpr) -> DataFrame: ...

    def limit(self, count: int, offset: int) -> DataFrame: ...

    def collect(self) -> List[pa.RecordBatch]: ...

    def cache(self) -> DataFrame: ...

    def collect_partitioned(self) -> List[List[pa.RecordBatch]]: ...

    def show(self, num: int = 20): ...

    def distinct(self) -> DataFrame: ...

    def join(self, right: DataFrame, how: str, left_on: List[str], right_on: List[str]) -> DataFrame: ...

    def join_on(self, right: DataFrame, on_exprs: List[Expr], how: str) -> DataFrame: ...

    def explain(self, verbose: bool = False, analyze: bool = False): ...

    def logical_plan(self) -> LogicalPlan: ...

    def optimized_logical_plan(self) -> LogicalPlan: ...

    def execution_plan(self) -> ExecutionPlan: ...

    def repartition(self, num: int) -> DataFrame: ...

    def repartition_by_hash(self, *args: Expr, num: int) -> DataFrame: ...

    def union(self, py_df: DataFrame, distinct: bool = False) -> DataFrame: ...

    def union_distinct(self, py_df: DataFrame) -> DataFrame: ...

    def unnest_column(self, column: str, preserve_nulls: bool = True) -> DataFrame: ...

    def unnest_columns(self, columns: List[str], preserve_nulls: bool = True) -> DataFrame: ...

    def intersect(self, py_df: DataFrame) -> DataFrame: ...

    def except_all(self, py_df: DataFrame) -> DataFrame: ...

    def write_csv(self, path: str, with_header: bool): ...

    def write_parquet(self, path: str, compression: str = "uncompressed", compression_level: Optional[int] = None): ...

    def write_json(self, path: str): ...

    def to_arrow_table(self) -> pa.Table: ...

    def __arrow_c_stream__(
        self, requested_schema: object | None = None
    ) -> object: ...

    def execute_stream(self) -> RecordBatchStream: ...

    def execute_stream_partitioned(self) -> List[RecordBatchStream]: ...

    def to_pandas(self) -> pd.DataFrame: ...

    def to_pylist(self) -> list: ...

    def to_pydict(self) -> dict: ...

    def to_polars(self) -> pl.DataFrame: ...

    def count(self) -> int: ...


class ScalarUDF:
    def __init__(
        self,
        name: str,
        func: Callable[..., pa.DataType],
        input_types: List[pa.DataType],
        return_type: pa.DataType,
        volatility: str
        ) -> None: ...

    def __call__(self, *args: Expr) -> Expr: ...

class AggregateUDF:
    def __init__(
        self,
        name: str,
        accumulator: Callable[[], Accumulator],
        input_types: List[pa.DataType],
        return_type: pa.DataType,
        state_type: List[pa.DataType],
        volatility: str,
        ) -> None: ...

    def __call__(self, *args: Expr) -> Expr: ...

class WindowUDF:
    def __init__(
        self,
        name: str,
        evaluator: Callable[[], WindowEvaluator],
        input_types: List[pa.DataType],
        return_type: pa.DataType,
        volatility: str,
        ) -> None: ...

    def __call__(self, *args: Expr) -> Expr: ...

class Config:
    def __init__(self) -> None: ...

    @staticmethod
    def from_env() -> Config: ...

    def get(self, key: str) -> Optional[str]:
        ...

    def set(self, key: str, value: object):
        ...

    def get_all(self) -> Dict[str, Optional[str]]:
        ...


class LogicalPlan:
    def to_variant(self) -> Any:
        ...

    def inputs(self) -> List[LogicalPlan]:
        ...

    def display(self) -> str:
        ...

    def display_indent(self) -> str:
        ...

    def display_indent_schema(self) -> str:
        ...

    def display_graphviz(self) -> str:
        ...

    def to_proto(self) -> bytes:
        ...

    @staticmethod
    def from_proto(ctx: SessionContext, proto_msg: bytes) -> LogicalPlan:
        ...

class ExecutionPlan:
    def children(self) -> List[ExecutionPlan]:
        ...

    def display(self) -> str:
        ...

    def display_indent(self) -> str:
        ...

    def to_proto(self) -> bytes:
        ...

    @staticmethod
    def from_proto(ctx: SessionContext, proto_msg: bytes) -> ExecutionPlan:
        ...

    @property
    def partition_count(self) -> int: ...


class RecordBatch:
    def to_pyarrow(self) -> pa.RecordBatch: ...


class RecordBatchStream:
    def next(self) -> RecordBatch:
        ...

    def __next__(self) -> RecordBatch:
        ...

    async def __anext__(self) -> RecordBatch:
        ...

    def __iter__(self) -> RecordBatch:
        ...

    async def __aiter__(self) -> RecordBatch:
        ...

