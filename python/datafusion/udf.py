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

"""Provides the user defined functions for evaluation of dataframes."""

from __future__ import annotations

import datafusion._internal as df_internal
from datafusion.expr import Expr
from typing import Callable, TYPE_CHECKING, TypeVar
from abc import ABCMeta, abstractmethod
from typing import List
from enum import Enum
import pyarrow

if TYPE_CHECKING:
    _R = TypeVar("_R", bound=pyarrow.DataType)


class Volatility(Enum):
    """Defines how stable or volatile a function is.

    When setting the volatility of a function, you can either pass this
    enumeration or a ``str``. The ``str`` equivalent is the lower case value of the
    name (`"immutable"`, `"stable"`, or `"volatile"`).
    """

    Immutable = 1
    """An immutable function will always return the same output when given the
    same input.

    DataFusion will attempt to inline immutable functions during planning.
    """

    Stable = 2
    """
    Returns the same value for a given input within a single queries.

    A stable function may return different values given the same input across
    different queries but must return the same value for a given input within a
    query. An example of this is the ``Now`` function. DataFusion will attempt to
    inline ``Stable`` functions during planning, when possible. For query
    ``select col1, now() from t1``, it might take a while to execute but ``now()``
    column will be the same for each output row, which is evaluated during
    planning.
    """

    Volatile = 3
    """A volatile function may change the return value from evaluation to
    evaluation.

    Multiple invocations of a volatile function may return different results
    when used in the same query. An example of this is the random() function.
    DataFusion can not evaluate such functions during planning. In the query
    ``select col1, random() from t1``, ``random()`` function will be evaluated
    for each output row, resulting in a unique random value for each row.
    """

    def __str__(self):
        """Returns the string equivalent."""
        return self.name.lower()


class ScalarUDF:
    """Class for performing scalar user defined functions (UDF).

    Scalar UDFs operate on a row by row basis. See also :py:class:`AggregateUDF` for
    operating on a group of rows.
    """

    def __init__(
        self,
        name: str | None,
        func: Callable[..., _R],
        input_types: list[pyarrow.DataType],
        return_type: _R,
        volatility: Volatility | str,
    ) -> None:
        """Instantiate a scalar user defined function (UDF).

        See helper method :py:func:`udf` for argument details.
        """
        self._udf = df_internal.ScalarUDF(
            name, func, input_types, return_type, str(volatility)
        )

    def __call__(self, *args: Expr) -> Expr:
        """Execute the UDF.

        This function is not typically called by an end user. These calls will
        occur during the evaluation of the dataframe.
        """
        args = [arg.expr for arg in args]
        return Expr(self._udf.__call__(*args))

    @staticmethod
    def udf(
        func: Callable[..., _R],
        input_types: list[pyarrow.DataType],
        return_type: _R,
        volatility: Volatility | str,
        name: str | None = None,
    ) -> ScalarUDF:
        """Create a new User Defined Function.

        Args:
            func: A callable python function.
            input_types: The data types of the arguments to ``func``. This list
                must be of the same length as the number of arguments.
            return_type: The data type of the return value from the python
                function.
            volatility: See ``Volatility`` for allowed values.
            name: A descriptive name for the function.

        Returns:
            A user defined aggregate function, which can be used in either data
                aggregation or window function calls.
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


class Accumulator(metaclass=ABCMeta):
    """Defines how an :py:class:`AggregateUDF` accumulates values."""

    @abstractmethod
    def state(self) -> List[pyarrow.Scalar]:
        """Return the current state."""
        pass

    @abstractmethod
    def update(self, values: pyarrow.Array) -> None:
        """Evaluate an array of values and update state."""
        pass

    @abstractmethod
    def merge(self, states: List[pyarrow.Array]) -> None:
        """Merge a set of states."""
        pass

    @abstractmethod
    def evaluate(self) -> pyarrow.Scalar:
        """Return the resultant value."""
        pass


if TYPE_CHECKING:
    _A = TypeVar("_A", bound=(Callable[..., _R], Accumulator))


class AggregateUDF:
    """Class for performing scalar user defined functions (UDF).

    Aggregate UDFs operate on a group of rows and return a single value. See
    also :py:class:`ScalarUDF` for operating on a row by row basis.
    """

    def __init__(
        self,
        name: str | None,
        accumulator: _A,
        input_types: list[pyarrow.DataType],
        return_type: _R,
        state_type: list[pyarrow.DataType],
        volatility: Volatility | str,
    ) -> None:
        """Instantiate a user defined aggregate function (UDAF).

        See :py:func:`udaf` for a convenience function and argument
        descriptions.
        """
        self._udf = df_internal.AggregateUDF(
            name, accumulator, input_types, return_type, state_type, str(volatility)
        )

    def __call__(self, *args: Expr) -> Expr:
        """Execute the UDAF.

        This function is not typically called by an end user. These calls will
        occur during the evaluation of the dataframe.
        """
        args = [arg.expr for arg in args]
        return Expr(self._udf.__call__(*args))

    @staticmethod
    def udaf(
        accum: _A,
        input_types: list[pyarrow.DataType],
        return_type: _R,
        state_type: list[pyarrow.DataType],
        volatility: Volatility | str,
        name: str | None = None,
    ) -> AggregateUDF:
        """Create a new User Defined Aggregate Function.

        The accumulator function must be callable and implement :py:class:`Accumulator`.

        Args:
            accum: The accumulator python function.
            input_types: The data types of the arguments to ``accum``.
            return_type: The data type of the return value.
            state_type: The data types of the intermediate accumulation.
            volatility: See :py:class:`Volatility` for allowed values.
            name: A descriptive name for the function.

        Returns:
            A user defined aggregate function, which can be used in either data
            aggregation or window function calls.
        """
        if not issubclass(accum, Accumulator):
            raise TypeError(
                "`accum` must implement the abstract base class Accumulator"
            )
        if name is None:
            name = accum.__qualname__.lower()
        if isinstance(input_types, pyarrow.lib.DataType):
            input_types = [input_types]
        return AggregateUDF(
            name=name,
            accumulator=accum,
            input_types=input_types,
            return_type=return_type,
            state_type=state_type,
            volatility=volatility,
        )
