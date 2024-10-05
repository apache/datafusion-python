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

try:
    import importlib.metadata as importlib_metadata
except ImportError:
    import importlib_metadata

from .context import (
    SessionContext,
    SessionConfig,
    RuntimeConfig,
    SQLOptions,
)

from .catalog import Catalog, Database, Table

# The following imports are okay to remain as opaque to the user.
from ._internal import Config

from .record_batch import RecordBatchStream, RecordBatch

from .udf import ScalarUDF, AggregateUDF, Accumulator, WindowUDF

from .common import (
    DFSchema,
)

from .dataframe import DataFrame

from .expr import (
    Expr,
    WindowFrame,
)

from .plan import LogicalPlan, ExecutionPlan

from . import functions, object_store, substrait

__version__ = importlib_metadata.version(__name__)

__all__ = [
    "Accumulator",
    "Config",
    "DataFrame",
    "SessionContext",
    "SessionConfig",
    "SQLOptions",
    "RuntimeConfig",
    "Expr",
    "ScalarUDF",
    "WindowFrame",
    "column",
    "col",
    "literal",
    "lit",
    "DFSchema",
    "Catalog",
    "Database",
    "Table",
    "AggregateUDF",
    "WindowUDF",
    "LogicalPlan",
    "ExecutionPlan",
    "RecordBatch",
    "RecordBatchStream",
    "common",
    "expr",
    "functions",
    "object_store",
    "substrait",
]


def column(value: str):
    """Create a column expression."""
    return Expr.column(value)


def col(value: str):
    """Create a column expression."""
    return Expr.column(value)


def literal(value):
    """Create a literal expression."""
    return Expr.literal(value)


def lit(value):
    """Create a literal expression."""
    return Expr.literal(value)


udf = ScalarUDF.udf

udaf = AggregateUDF.udaf

udwf = WindowUDF.udwf
