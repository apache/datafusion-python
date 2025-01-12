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

import enum
from typing import List, Optional, Tuple

class DFSchema:
    @staticmethod
    def empty() -> DFSchema:
        ...

    def field_names(self) -> List[str]:
        ...

class DataType:
    ...


class RexType(enum.IntEnum):
    Alias = 0
    Literal = 1
    Call = 2
    Reference = 3
    ScalarSubquery = 4
    Other = 5


class PythonType(enum.IntEnum):
    Array = 0
    Bool = 1
    Bytes = 2
    Datetime = 3
    Float = 4
    Int = 5
    List = 6
    None_ = 7
    Object = 8
    Str = 9


class SqlType(enum.IntEnum):
    ANY = 0
    ARRAY = 1
    BIGINT = 2
    BINARY = 3
    BOOLEAN = 4
    CHAR = 5
    COLUMN_LIST = 6
    CURSOR = 7
    DATE = 8
    DECIMAL = 9
    DISTINCT = 10
    DOUBLE = 11
    DYNAMIC_STAR = 12
    FLOAT = 13
    GEOMETRY = 14
    INTEGER = 15
    INTERVAL = 16
    INTERVAL_DAY = 17
    INTERVAL_DAY_HOUR = 18
    INTERVAL_DAY_MINUTE = 19
    INTERVAL_DAY_SECOND = 20
    INTERVAL_HOUR = 21
    INTERVAL_HOUR_MINUTE = 22
    INTERVAL_HOUR_SECOND = 23
    INTERVAL_MINUTE = 24
    INTERVAL_MINUTE_SECOND = 25
    INTERVAL_MONTH = 26
    INTERVAL_SECOND = 27
    INTERVAL_YEAR = 28
    INTERVAL_YEAR_MONTH = 29
    MAP = 30
    MULTISET = 31
    NULL = 32
    OTHER = 33
    REAL = 34
    ROW = 35
    SARG = 36
    SMALLINT = 37
    STRUCTURED = 38
    SYMBOL = 39
    TIME = 40
    TIME_WITH_LOCAL_TIME_ZONE = 41
    TIMESTAMP = 42
    TIMESTAMP_WITH_LOCAL_TIME_ZONE = 43
    TINYINT = 44
    UNKNOWN = 45
    VARBINARY = 46
    VARCHAR = 47


class DataTypeMap:
    def __init__(self, arrow_type: DataType, python_type: PythonType, sql_type: SqlType) -> None: ...

    @staticmethod
    def from_parquet_type_str(parquet_str_type: str) -> DataTypeMap: ...

    @staticmethod
    def arrow(arrow_type: DataType) -> DataTypeMap: ...

    @staticmethod
    def arrow_str(arrow_type_str: str) -> DataTypeMap: ...

    @staticmethod
    def sql(sql_type: SqlType) -> DataTypeMap: ...

    def friendly_arrow_type_name(self) -> str: ...

    @property
    def arrow_type(self) -> DataType: ...

    @arrow_type.setter
    def arrow_type(self, arrow_type: DataType): ...

    @property
    def python_type(self) -> PythonType: ...

    @python_type.setter
    def python_type(self, python_type: PythonType): ...

    @property
    def sql_type(self) -> SqlType: ...

    @sql_type.setter
    def sql_type(self, sql_type: SqlType): ...


class NullTreatment(enum.IntEnum):
    IGNORE_NULLS = 0
    RESPECT_NULLS = 1


class SqlSchema:

    def __init__(self, schema_name: str) -> None: ...

    def table_by_name(self, table_name: str) -> Optional[SqlTable]: ...

    def add_table(self, table: SqlTable): ...

    def drop_table(self, table_name: str): ...

    @property
    def name(self) -> str: ...

    @name.setter
    def name(self, name: str): ...

    @property
    def tables(self) -> List[SqlTable]: ...

    @tables.setter
    def tables(self, tables: List[SqlTable]): ...

    @property
    def views(self) -> List[SqlView]: ...

    @views.setter
    def views(self, views: List[SqlView]): ...

    @property
    def functions(self) -> List[SqlFunction]: ...

    @functions.setter
    def functions(self, functions: List[SqlFunction]): ...


class SqlTable:
    def __init__(
        self,
        table_name: str,
        columns: List[Tuple[str, DataTypeMap]],
        row_count: int,
        filepaths: Optional[List[str]] = None
        ) -> None: ...

    @property
    def name(self) -> str: ...

    @name.setter
    def name(self, name: str): ...

    @property
    def columns(self) -> List[Tuple[str, DataTypeMap]]: ...

    @columns.setter
    def columns(self, columns: List[Tuple[str, DataTypeMap]]): ...

    @property
    def primary_key(self) -> Optional[str]: ...

    @primary_key.setter
    def primary_key(self, primary_key: Optional[str]): ...

    @property
    def foreign_keys(self) -> List[str]: ...

    @foreign_keys.setter
    def foreign_keys(self, foreign_keys: List[str]): ...

    @property
    def indexes(self) -> List[str]: ...

    @indexes.setter
    def indexes(self, indexes: List[str]): ...

    @property
    def constraints(self) -> List[str]: ...

    @constraints.setter
    def constraints(self, constraints: List[str]): ...

    @property
    def statistics(self) -> SqlStatistics: ...

    @statistics.setter
    def statistics(self, statistics: SqlStatistics): ...

    @property
    def filepaths(self) -> Optional[List[str]]: ...

    @filepaths.setter
    def filepaths(self, filepaths: Optional[List[str]]): ...


class SqlView:

    @property
    def name(self) -> str: ...

    @name.setter
    def name(self, name: str): ...

    @property
    def definition(self) -> str: ...

    @definition.setter
    def definition(self, definition: str): ...


class SqlStatistics:
    def __init__(self, row_count: float) -> None: ...

    def getRowCount(self) -> float: ...


class SqlFunction:
    ...
