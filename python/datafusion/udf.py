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

"""Provides the user-defined functions for evaluation of dataframes."""

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
    """Class for performing scalar user-defined functions (UDF).

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
        """Instantiate a scalar user-defined function (UDF).

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
        """Create a new User-Defined Function.

        Args:
            func: A callable python function.
            input_types: The data types of the arguments to ``func``. This list
                must be of the same length as the number of arguments.
            return_type: The data type of the return value from the python
                function.
            volatility: See ``Volatility`` for allowed values.
            name: A descriptive name for the function.

        Returns:
            A user-defined aggregate function, which can be used in either data
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
    def update(self, *values: pyarrow.Array) -> None:
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
    """Class for performing scalar user-defined functions (UDF).

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
        """Instantiate a user-defined aggregate function (UDAF).

        See :py:func:`udaf` for a convenience function and argument
        descriptions.
        """
        self._udaf = df_internal.AggregateUDF(
            name, accumulator, input_types, return_type, state_type, str(volatility)
        )

    def __call__(self, *args: Expr) -> Expr:
        """Execute the UDAF.

        This function is not typically called by an end user. These calls will
        occur during the evaluation of the dataframe.
        """
        args = [arg.expr for arg in args]
        return Expr(self._udaf.__call__(*args))

    @staticmethod
    def udaf(
        accum: _A,
        input_types: list[pyarrow.DataType],
        return_type: _R,
        state_type: list[pyarrow.DataType],
        volatility: Volatility | str,
        name: str | None = None,
    ) -> AggregateUDF:
        """Create a new User-Defined Aggregate Function.

        The accumulator function must be callable and implement :py:class:`Accumulator`.

        Args:
            accum: The accumulator python function.
            input_types: The data types of the arguments to ``accum``.
            return_type: The data type of the return value.
            state_type: The data types of the intermediate accumulation.
            volatility: See :py:class:`Volatility` for allowed values.
            name: A descriptive name for the function.

        Returns:
            A user-defined aggregate function, which can be used in either data
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


class WindowEvaluator(metaclass=ABCMeta):
    """Evaluator class for user-defined window functions (UDWF).

    It is up to the user to decide which evaluate function is appropriate.

    +------------------------+--------------------------------+------------------+---------------------------+
    | ``uses_window_frame``  | ``supports_bounded_execution`` | ``include_rank`` | function_to_implement     |
    +========================+================================+==================+===========================+
    | False (default)        | False (default)                | False (default)  | ``evaluate_all``          |
    +------------------------+--------------------------------+------------------+---------------------------+
    | False                  | True                           | False            | ``evaluate``              |
    +------------------------+--------------------------------+------------------+---------------------------+
    | False                  | True/False                     | True             | ``evaluate_all_with_rank``|
    +------------------------+--------------------------------+------------------+---------------------------+
    | True                   | True/False                     | True/False       | ``evaluate``              |
    +------------------------+--------------------------------+------------------+---------------------------+
    """  # noqa: W505

    def memoize(self) -> None:
        """Perform a memoize operation to improve performance.

        When the window frame has a fixed beginning (e.g UNBOUNDED
        PRECEDING), some functions such as FIRST_VALUE, LAST_VALUE and
        NTH_VALUE do not need the (unbounded) input once they have
        seen a certain amount of input.

        `memoize` is called after each input batch is processed, and
        such functions can save whatever they need
        """
        pass

    def get_range(self, idx: int, num_rows: int) -> tuple[int, int]:
        """Return the range for the window fuction.

        If `uses_window_frame` flag is `false`. This method is used to
        calculate required range for the window function during
        stateful execution.

        Generally there is no required range, hence by default this
        returns smallest range(current row). e.g seeing current row is
        enough to calculate window result (such as row_number, rank,
        etc)

        Args:
            idx:: Current index
            num_rows: Number of rows.
        """
        return (idx, idx + 1)

    def is_causal(self) -> bool:
        """Get whether evaluator needs future data for its result."""
        return False

    def evaluate_all(self, values: list[pyarrow.Array], num_rows: int) -> pyarrow.Array:
        """Evaluate a window function on an entire input partition.

        This function is called once per input *partition* for window functions that
        *do not use* values from the window frame, such as
        :py:func:`~datafusion.functions.row_number`, :py:func:`~datafusion.functions.rank`,
        :py:func:`~datafusion.functions.dense_rank`, :py:func:`~datafusion.functions.percent_rank`,
        :py:func:`~datafusion.functions.cume_dist`, :py:func:`~datafusion.functions.lead`,
        and :py:func:`~datafusion.functions.lag`.

        It produces the result of all rows in a single pass. It
        expects to receive the entire partition as the ``value`` and
        must produce an output column with one output row for every
        input row.

        ``num_rows`` is required to correctly compute the output in case
        ``len(values) == 0``

        Implementing this function is an optimization. Certain window
        functions are not affected by the window frame definition or
        the query doesn't have a frame, and ``evaluate`` skips the
        (costly) window frame boundary calculation and the overhead of
        calling ``evaluate`` for each output row.

        For example, the `LAG` built in window function does not use
        the values of its window frame (it can be computed in one shot
        on the entire partition with ``Self::evaluate_all`` regardless of the
        window defined in the ``OVER`` clause)

        .. code-block:: text

            lag(x, 1) OVER (ORDER BY z ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING)

        However, ``avg()`` computes the average in the window and thus
        does use its window frame.

        .. code-block:: text

            avg(x) OVER (PARTITION BY y ORDER BY z ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING)
        """  # noqa: W505
        pass

    def evaluate(
        self, values: list[pyarrow.Array], eval_range: tuple[int, int]
    ) -> pyarrow.Scalar:
        """Evaluate window function on a range of rows in an input partition.

        This is the simplest and most general function to implement
        but also the least performant as it creates output one row at
        a time. It is typically much faster to implement stateful
        evaluation using one of the other specialized methods on this
        trait.

        Returns a [`ScalarValue`] that is the value of the window
        function within `range` for the entire partition. Argument
        `values` contains the evaluation result of function arguments
        and evaluation results of ORDER BY expressions. If function has a
        single argument, `values[1..]` will contain ORDER BY expression results.
        """
        pass

    def evaluate_all_with_rank(
        self, num_rows: int, ranks_in_partition: list[tuple[int, int]]
    ) -> pyarrow.Array:
        """Called for window functions that only need the rank of a row.

        Evaluate the partition evaluator against the partition using
        the row ranks. For example, ``rank(col("a"))`` produces

        .. code-block:: text

            a | rank
            - + ----
            A | 1
            A | 1
            C | 3
            D | 4
            D | 4

        For this case, `num_rows` would be `5` and the
        `ranks_in_partition` would be called with

        .. code-block:: text

            [
                (0,1),
                (2,2),
                (3,4),
            ]

        The user must implement this method if ``include_rank`` returns True.
        """
        pass

    def supports_bounded_execution(self) -> bool:
        """Can the window function be incrementally computed using bounded memory?"""
        return False

    def uses_window_frame(self) -> bool:
        """Does the window function use the values from the window frame?"""
        return False

    def include_rank(self) -> bool:
        """Can this function be evaluated with (only) rank?"""
        return False


if TYPE_CHECKING:
    _W = TypeVar("_W", bound=WindowEvaluator)


class WindowUDF:
    """Class for performing window user-defined functions (UDF).

    Window UDFs operate on a partition of rows. See
    also :py:class:`ScalarUDF` for operating on a row by row basis.
    """

    def __init__(
        self,
        name: str | None,
        func: WindowEvaluator,
        input_types: list[pyarrow.DataType],
        return_type: pyarrow.DataType,
        volatility: Volatility | str,
    ) -> None:
        """Instantiate a user-defined window function (UDWF).

        See :py:func:`udwf` for a convenience function and argument
        descriptions.
        """
        self._udwf = df_internal.WindowUDF(
            name, func, input_types, return_type, str(volatility)
        )

    def __call__(self, *args: Expr) -> Expr:
        """Execute the UDWF.

        This function is not typically called by an end user. These calls will
        occur during the evaluation of the dataframe.
        """
        args_raw = [arg.expr for arg in args]
        return Expr(self._udwf.__call__(*args_raw))

    @staticmethod
    def udwf(
        func: WindowEvaluator,
        input_types: pyarrow.DataType | list[pyarrow.DataType],
        return_type: pyarrow.DataType,
        volatility: Volatility | str,
        name: str | None = None,
    ) -> WindowUDF:
        """Create a new User-Defined Window Function.

        Args:
            func: The python function.
            input_types: The data types of the arguments to ``func``.
            return_type: The data type of the return value.
            volatility: See :py:class:`Volatility` for allowed values.
            name: A descriptive name for the function.

        Returns:
            A user-defined window function.
        """
        if not isinstance(func, WindowEvaluator):
            raise TypeError(
                "`func` must implement the abstract base class WindowEvaluator"
            )
        if name is None:
            name = func.__class__.__qualname__.lower()
        if isinstance(input_types, pyarrow.DataType):
            input_types = [input_types]
        return WindowUDF(
            name=name,
            func=func,
            input_types=input_types,
            return_type=return_type,
            volatility=volatility,
        )
