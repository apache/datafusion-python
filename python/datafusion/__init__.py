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

"""DataFusion python package.

This is a Python library that binds to Apache Arrow in-memory query engine DataFusion.
See https://datafusion.apache.org/python for more information.
"""

from __future__ import annotations

from typing import Any

try:
    import importlib.metadata as importlib_metadata
except ImportError:
    import importlib_metadata

from . import functions, object_store, substrait, unparser

# The following imports are okay to remain as opaque to the user.
from ._internal import Config
from .catalog import Catalog, Database, Table
from .col import col, column
from .common import (
    DFSchema,
)
from .context import (
    RuntimeEnvBuilder,
    SessionConfig,
    SessionContext,
    SQLOptions,
)
from .dataframe import DataFrame, ParquetColumnOptions, ParquetWriterOptions
from .dataframe_formatter import configure_formatter
from .expr import (
    Expr,
    WindowFrame,
)
from .io import read_avro, read_csv, read_json, read_parquet
from .plan import ExecutionPlan, LogicalPlan
from .record_batch import RecordBatch, RecordBatchStream
from .user_defined import (
    Accumulator,
    AggregateUDF,
    ScalarUDF,
    TableFunction,
    WindowUDF,
    udaf,
    udf,
    udtf,
    udwf,
)

__version__ = importlib_metadata.version(__name__)

__all__ = [
    "Accumulator",
    "AggregateUDF",
    "Catalog",
    "Config",
    "DFSchema",
    "DataFrame",
    "Database",
    "ExecutionPlan",
    "Expr",
    "LogicalPlan",
    "ParquetColumnOptions",
    "ParquetWriterOptions",
    "RecordBatch",
    "RecordBatchStream",
    "RuntimeEnvBuilder",
    "SQLOptions",
    "ScalarUDF",
    "SessionConfig",
    "SessionContext",
    "Table",
    "TableFunction",
    "WindowFrame",
    "WindowUDF",
    "catalog",
    "col",
    "column",
    "common",
    "configure_formatter",
    "expr",
    "functions",
    "lit",
    "literal",
    "object_store",
    "read_avro",
    "read_csv",
    "read_json",
    "read_parquet",
    "substrait",
    "udaf",
    "udf",
    "udtf",
    "udwf",
    "unparser",
]


def literal(value) -> Expr:
    """Create a literal expression."""
    return Expr.literal(value)


def string_literal(value):
    """Create a UTF8 literal expression.

    It differs from `literal` which creates a UTF8view literal.
    """
    return Expr.string_literal(value)


def str_lit(value):
    """Alias for `string_literal`."""
    return string_literal(value)


def lit(value) -> Expr:
    """Create a literal expression."""
    return Expr.literal(value)


def literal_with_metadata(value: Any, metadata: dict[str, str]) -> Expr:
    """Creates a new expression representing a scalar value with metadata.

    Args:
        value: A valid PyArrow scalar value or easily castable to one.
        metadata: Metadata to attach to the expression.
    """
    return Expr.literal_with_metadata(value, metadata)


def lit_with_metadata(value: Any, metadata: dict[str, str]) -> Expr:
    """Alias for literal_with_metadata."""
    return literal_with_metadata(value, metadata)
