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

from abc import ABCMeta, abstractmethod
from typing import List

try:
    import importlib.metadata as importlib_metadata
except ImportError:
    import importlib_metadata

import pyarrow as pa

from ._internal import (
    AggregateUDF,
    Config,
    DataFrame,
    SessionContext,
    SessionConfig,
    RuntimeConfig,
    ScalarUDF,
    SQLOptions,
)

from .common import (
    DFSchema,
)

from .expr import (
    Alias,
    Analyze,
    Expr,
    Filter,
    Limit,
    Like,
    ILike,
    Projection,
    SimilarTo,
    ScalarVariable,
    Sort,
    TableScan,
    Not,
    IsNotNull,
    IsTrue,
    IsFalse,
    IsUnknown,
    IsNotTrue,
    IsNotFalse,
    IsNotUnknown,
    Negative,
    InList,
    Exists,
    Subquery,
    InSubquery,
    ScalarSubquery,
    GroupingSet,
    Placeholder,
    Case,
    Cast,
    TryCast,
    Between,
    Explain,
    CreateMemoryTable,
    SubqueryAlias,
    Extension,
    CreateView,
    Distinct,
    DropTable,
    Repartition,
    Partitioning,
    Window,
    WindowFrame,
)

__version__ = importlib_metadata.version(__name__)


class Accumulator(metaclass=ABCMeta):
    @abstractmethod
    def state(self) -> List[pa.Scalar]:
        pass

    @abstractmethod
    def update(self, values: pa.Array) -> None:
        pass

    @abstractmethod
    def merge(self, states: pa.Array) -> None:
        pass

    @abstractmethod
    def evaluate(self) -> pa.Scalar:
        pass


def column(value):
    return Expr.column(value)


col = column


def literal(value):
    import pyarrow as pa

    if not isinstance(value, pa.Scalar):
        value = pa.scalar(value)
    return Expr.literal(value)


lit = literal


def udf(func, input_types, return_type, volatility, name=None):
    """
    Create a new User Defined Function
    """
    if not callable(func):
        raise TypeError("`func` argument must be callable")
    if name is None:
        name = func.__qualname__.lower()
    return ScalarUDF(
        name=name,
        func=func,
        input_types=input_types,
        return_type=return_type,
        volatility=volatility,
    )


def udaf(accum, input_type, return_type, state_type, volatility, name=None):
    """
    Create a new User Defined Aggregate Function
    """
    import pyarrow as pa

    if not issubclass(accum, Accumulator):
        raise TypeError("`accum` must implement the abstract base class Accumulator")
    if name is None:
        name = accum.__qualname__.lower()
    if isinstance(input_type, pa.lib.DataType):
        input_type = [input_type]
    return AggregateUDF(
        name=name,
        accumulator=accum,
        input_type=input_type,
        return_type=return_type,
        state_type=state_type,
        volatility=volatility,
    )


del ABCMeta
del abstractmethod
del List
del importlib_metadata
del pa


__all__ = [
    "Config",
    "DataFrame",
    "SessionContext",
    "SessionConfig",
    "SQLOptions",
    "RuntimeConfig",
    "Expr",
    "AggregateUDF",
    "ScalarUDF",
    "Window",
    "WindowFrame",
    "column",
    "literal",
    "TableScan",
    "Projection",
    "DFSchema",
    "Analyze",
    "Sort",
    "Limit",
    "Filter",
    "Like",
    "ILike",
    "SimilarTo",
    "ScalarVariable",
    "Alias",
    "Not",
    "IsNotNull",
    "IsTrue",
    "IsFalse",
    "IsUnknown",
    "IsNotTrue",
    "IsNotFalse",
    "IsNotUnknown",
    "Negative",
    "InList",
    "Exists",
    "Subquery",
    "InSubquery",
    "ScalarSubquery",
    "GroupingSet",
    "Placeholder",
    "Case",
    "Cast",
    "TryCast",
    "Between",
    "Explain",
    "SubqueryAlias",
    "Extension",
    "CreateMemoryTable",
    "CreateView",
    "Distinct",
    "DropTable",
    "Repartition",
    "Partitioning",
]
