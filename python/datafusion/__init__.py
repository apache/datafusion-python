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
from .col import col, column
from .context import (
    RuntimeEnvBuilder,
    SessionConfig,
    SessionContext,
)
from .dataframe import DataFrame
from .expr import Expr
from .extensions import SessionExtensionComponents
from .io import read_avro, read_csv, read_json, read_parquet
from .user_defined import (
    Accumulator,
    udaf,
    udf,
    udtf,
    udwf,
)

__version__ = importlib_metadata.version(__name__)

__all__ = [
    "Accumulator",
    "DataFrame",
    "Expr",
    "RuntimeEnvBuilder",
    "SessionConfig",
    "SessionContext",
    "SessionExtensionComponents",
    "col",
    "column",
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
    "Table": ("datafusion.catalog", "Table"),
    "TableProviderFactory": ("datafusion.catalog", "TableProviderFactory"),
    "TableProviderFactoryExportable": (
        "datafusion.catalog",
        "TableProviderFactoryExportable",
    ),
    # datafusion.common
    "DFSchema": ("datafusion.common", "DFSchema"),
    # datafusion.expr
    "WindowFrame": ("datafusion.expr", "WindowFrame"),
    "string_literal": ("datafusion.expr", "string_literal"),
    "str_lit": ("datafusion.expr", "str_lit"),
    # datafusion.context
    "SQLOptions": ("datafusion.context", "SQLOptions"),
    # datafusion.options
    "CsvReadOptions": ("datafusion.options", "CsvReadOptions"),
    # datafusion.record_batch
    "RecordBatch": ("datafusion.record_batch", "RecordBatch"),
    "RecordBatchStream": ("datafusion.record_batch", "RecordBatchStream"),
    # datafusion.user_defined
    "AggregateUDF": ("datafusion.user_defined", "AggregateUDF"),
    "ScalarUDF": ("datafusion.user_defined", "ScalarUDF"),
    "TableFunction": ("datafusion.user_defined", "TableFunction"),
    "WindowUDF": ("datafusion.user_defined", "WindowUDF"),
    # datafusion.dataframe_formatter
    "configure_formatter": ("datafusion.dataframe_formatter", "configure_formatter"),
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
    msg = f"module '{__name__}' has no attribute '{name}'"
    raise AttributeError(msg)


def literal(value: Any) -> Expr:
    """Create a literal expression."""
    return Expr.literal(value)


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
