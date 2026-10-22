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

"""DataFusion: an in-process query engine built on Apache Arrow.

DataFusion is not a database -- it has no server and no external dependencies.
You create a :py:class:`SessionContext`, point it at data sources (Parquet, CSV,
JSON, Arrow IPC, Pandas, Polars, or raw Python dicts/lists), and run queries
using either SQL or the DataFrame API.

Core abstractions
-----------------
- **SessionContext** -- entry point for loading data, running SQL, and creating
  DataFrames.
- **DataFrame** -- lazy query builder. Every method returns a new DataFrame;
  call :py:meth:`~datafusion.dataframe.DataFrame.collect` or a ``to_*``
  method to execute.
- **Expr** -- expression tree node for column references, literals, and function
  calls. Build with :py:func:`col` and :py:func:`lit`.
- **functions** -- 290+ built-in scalar, aggregate, and window functions.

Quick start
-----------

>>> from datafusion import SessionContext, col
>>> from datafusion import functions as F
>>> ctx = SessionContext()
>>> df = ctx.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
>>> result = (
...     df.filter(col("a") > 1)
...       .with_column("total", col("a") + col("b"))
...       .aggregate([], [F.sum(col("total")).alias("grand_total")])
... )
>>> result.to_pydict()
{'grand_total': [16]}

User guide and full documentation: https://datafusion.apache.org/python

AI agent reference (SQL-to-DataFrame mappings, expression-building patterns,
common pitfalls), written in a dense, skill-oriented format:
https://github.com/apache/datafusion-python/blob/main/skills/datafusion_python/SKILL.md
"""

from __future__ import annotations

import importlib
import importlib.metadata as importlib_metadata
import warnings
from typing import Any

# Public submodules
from . import functions, ipc, object_store, substrait, unparser
from .catalog import Table
from .col import col, column
from .context import (
    RuntimeEnvBuilder,
    SessionConfig,
    SessionContext,
)
from .dataframe import DataFrame
from .dataframe_formatter import configure_formatter
from .expr import Expr
from .extensions import SessionExtensionComponents
from .io import read_avro, read_csv, read_json, read_parquet
from .options import CsvReadOptions
from .record_batch import RecordBatch
from .user_defined import (
    Accumulator,
    AggregateUDF,
    ScalarUDF,
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
    "CsvReadOptions",
    "DataFrame",
    "Expr",
    "RecordBatch",
    "RuntimeEnvBuilder",
    "ScalarUDF",
    "SessionConfig",
    "SessionContext",
    "SessionExtensionComponents",
    "Table",
    "WindowUDF",
    "col",
    "column",
    "configure_formatter",
    "functions",
    "ipc",
    "lit",
    "lit_with_metadata",
    "literal",
    "literal_with_metadata",
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

_DEPRECATED_ROOT_NAMES: dict[str, tuple[str, str]] = {
    # datafusion.dataframe
    "DataFrameWriteOptions": ("datafusion.dataframe", "DataFrameWriteOptions"),
    "ExplainFormat": ("datafusion.dataframe", "ExplainFormat"),
    "InsertOp": ("datafusion.dataframe", "InsertOp"),
    "ParquetColumnOptions": ("datafusion.dataframe", "ParquetColumnOptions"),
    "ParquetWriterOptions": ("datafusion.dataframe", "ParquetWriterOptions"),
    # datafusion.plan
    "ExecutionPlan": ("datafusion.plan", "ExecutionPlan"),
    "LogicalPlan": ("datafusion.plan", "LogicalPlan"),
    "Metric": ("datafusion.plan", "Metric"),
    "MetricsSet": ("datafusion.plan", "MetricsSet"),
    "PhysicalPartitioning": ("datafusion.plan", "PhysicalPartitioning"),
    # datafusion.catalog
    "Catalog": ("datafusion.catalog", "Catalog"),
    "TableProviderFactory": ("datafusion.catalog", "TableProviderFactory"),
    "TableProviderFactoryExportable": (
        "datafusion.catalog",
        "TableProviderFactoryExportable",
    ),
    # datafusion.common
    "DFSchema": ("datafusion.common", "DFSchema"),
    # datafusion.expr
    "WindowFrame": ("datafusion.expr", "WindowFrame"),
    # datafusion.context
    "SQLOptions": ("datafusion.context", "SQLOptions"),
    # datafusion.record_batch
    "RecordBatchStream": ("datafusion.record_batch", "RecordBatchStream"),
    # datafusion.user_defined
    "TableFunction": ("datafusion.user_defined", "TableFunction"),
}

_SUBMODULES: set[str] = {
    "catalog",
    "col",
    "common",
    "context",
    "dataframe",
    "dataframe_formatter",
    "expr",
    "extensions",
    "functions",
    "input",
    "io",
    "ipc",
    "object_store",
    "options",
    "plan",
    "record_batch",
    "substrait",
    "unparser",
    "user_defined",
}


def __getattr__(name: str) -> Any:
    if name in _DEPRECATED_ROOT_NAMES:
        module_path, target_name = _DEPRECATED_ROOT_NAMES[name]
        warnings.warn(
            f"`datafusion.{name}` is deprecated and will be removed in a future "
            f"release. Import `{target_name}` from `{module_path}` instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        module = importlib.import_module(module_path)
        return getattr(module, target_name)
    if name in _SUBMODULES:
        return importlib.import_module(f".{name}", __name__)
    msg = f"module '{__name__}' has no attribute '{name}'"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    return sorted(__all__)


def literal(value: Any) -> Expr:
    """Create a literal expression."""
    return Expr.literal(value)


def string_literal(value: str) -> Expr:
    """Create a UTF8 literal expression.

    It differs from `literal` which creates a UTF8view literal.
    """
    return Expr.string_literal(value)


def str_lit(value: str) -> Expr:
    """Alias for `string_literal`."""
    return string_literal(value)


def lit(value: Any) -> Expr:
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
