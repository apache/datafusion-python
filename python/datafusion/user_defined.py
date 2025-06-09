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

import functools
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Optional, Protocol, TypeVar, overload

import pyarrow as pa

import datafusion._internal as df_internal
from datafusion.expr import Expr

if TYPE_CHECKING:
    _R = TypeVar("_R", bound=pa.DataType)


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

    def __str__(self) -> str:
        """Returns the string equivalent."""
        return self.name.lower()


class ScalarUDFExportable(Protocol):
    """Type hint for object that has __datafusion_scalar_udf__ PyCapsule."""

    def __datafusion_scalar_udf__(self) -> object: ...  # noqa: D105


class ScalarUDF:
    """Class for performing scalar user-defined functions (UDF).

    Scalar UDFs operate on a row by row basis. See also :py:class:`AggregateUDF` for
    operating on a group of rows.
    """

    def __init__(
        self,
        name: str,
        func: Callable[..., _R],
        input_types: pa.DataType | list[pa.DataType],
        return_type: _R,
        volatility: Volatility | str,
    ) -> None:
        """Instantiate a scalar user-defined function (UDF).

        See helper method :py:func:`udf` for argument details.
        """
        if hasattr(func, "__datafusion_scalar_udf__"):
            self._udf = df_internal.ScalarUDF.from_pycapsule(func)
            return
        if isinstance(input_types, pa.DataType):
            input_types = [input_types]
        self._udf = df_internal.ScalarUDF(
            name, func, input_types, return_type, str(volatility)
        )

    def __repr__(self) -> str:
        """Print a string representation of the Scalar UDF."""
        return self._udf.__repr__()

    def __call__(self, *args: Expr) -> Expr:
        """Execute the UDF.

        This function is not typically called by an end user. These calls will
        occur during the evaluation of the dataframe.
        """
        args_raw = [arg.expr for arg in args]
        return Expr(self._udf.__call__(*args_raw))

    @overload
    @staticmethod
    def udf(
        input_types: list[pa.DataType],
        return_type: _R,
        volatility: Volatility | str,
        name: Optional[str] = None,
    ) -> Callable[..., ScalarUDF]: ...

    @overload
    @staticmethod
    def udf(
        func: Callable[..., _R],
        input_types: list[pa.DataType],
        return_type: _R,
        volatility: Volatility | str,
        name: Optional[str] = None,
    ) -> ScalarUDF: ...

    @overload
    @staticmethod
    def udf(func: ScalarUDFExportable) -> ScalarUDF: ...

    @staticmethod
    def udf(*args: Any, **kwargs: Any):  # noqa: D417
        """Create a new User-Defined Function (UDF).

        This class can be used both as either a function or a decorator.

        Usage:
            - As a function: ``udf(func, input_types, return_type, volatility, name)``.
            - As a decorator: ``@udf(input_types, return_type, volatility, name)``.
              When used a decorator, do **not** pass ``func`` explicitly.

        Args:
            func (Callable, optional): Only needed when calling as a function.
                Skip this argument when using `udf` as a decorator. If you have a Rust
                backed ScalarUDF within a PyCapsule, you can pass this parameter
                and ignore the rest. They will be determined directly from the
                underlying function. See the online documentation for more information.
            input_types (list[pa.DataType]): The data types of the arguments
                to ``func``. This list must be of the same length as the number of
                arguments.
            return_type (_R): The data type of the return value from the function.
            volatility (Volatility | str): See `Volatility` for allowed values.
            name (Optional[str]): A descriptive name for the function.

        Returns:
            A user-defined function that can be used in SQL expressions,
            data aggregation, or window function calls.

        Example: Using ``udf`` as a function::

            def double_func(x):
                return x * 2
            double_udf = udf(double_func, [pa.int32()], pa.int32(),
            "volatile", "double_it")

        Example: Using ``udf`` as a decorator::

            @udf([pa.int32()], pa.int32(), "volatile", "double_it")
            def double_udf(x):
                return x * 2
        """

        def _function(
            func: Callable[..., _R],
            input_types: list[pa.DataType],
            return_type: _R,
            volatility: Volatility | str,
            name: Optional[str] = None,
        ) -> ScalarUDF:
            if not callable(func):
                msg = "`func` argument must be callable"
                raise TypeError(msg)
            if name is None:
                if hasattr(func, "__qualname__"):
                    name = func.__qualname__.lower()
                else:
                    name = func.__class__.__name__.lower()
            return ScalarUDF(
                name=name,
                func=func,
                input_types=input_types,
                return_type=return_type,
                volatility=volatility,
            )

        def _decorator(
            input_types: list[pa.DataType],
            return_type: _R,
            volatility: Volatility | str,
            name: Optional[str] = None,
        ) -> Callable:
            def decorator(func: Callable):
                udf_caller = ScalarUDF.udf(
                    func, input_types, return_type, volatility, name
                )

                @functools.wraps(func)
                def wrapper(*args: Any, **kwargs: Any):
                    return udf_caller(*args, **kwargs)

                return wrapper

            return decorator

        if hasattr(args[0], "__datafusion_scalar_udf__"):
            return ScalarUDF.from_pycapsule(args[0])

        if args and callable(args[0]):
            # Case 1: Used as a function, require the first parameter to be callable
            return _function(*args, **kwargs)
        # Case 2: Used as a decorator with parameters
        return _decorator(*args, **kwargs)

    @staticmethod
    def from_pycapsule(func: ScalarUDFExportable) -> ScalarUDF:
        """Create a Scalar UDF from ScalarUDF PyCapsule object.

        This function will instantiate a Scalar UDF that uses a DataFusion
        ScalarUDF that is exported via the FFI bindings.
        """
        name = str(func.__class__)
        return ScalarUDF(
            name=name,
            func=func,
            input_types=None,
            return_type=None,
            volatility=None,
        )


class Accumulator(metaclass=ABCMeta):
    """Defines how an :py:class:`AggregateUDF` accumulates values."""

    @abstractmethod
    def state(self) -> list[pa.Scalar]:
        """Return the current state."""

    @abstractmethod
    def update(self, *values: pa.Array) -> None:
        """Evaluate an array of values and update state."""

    @abstractmethod
    def merge(self, states: list[pa.Array]) -> None:
        """Merge a set of states."""

    @abstractmethod
    def evaluate(self) -> pa.Scalar:
        """Return the resultant value."""


class AggregateUDFExportable(Protocol):
    """Type hint for object that has __datafusion_aggregate_udf__ PyCapsule."""

    def __datafusion_aggregate_udf__(self) -> object: ...  # noqa: D105


class AggregateUDF:
    """Class for performing scalar user-defined functions (UDF).

    Aggregate UDFs operate on a group of rows and return a single value. See
    also :py:class:`ScalarUDF` for operating on a row by row basis.
    """

    def __init__(
        self,
        name: str,
        accumulator: Callable[[], Accumulator],
        input_types: list[pa.DataType],
        return_type: pa.DataType,
        state_type: list[pa.DataType],
        volatility: Volatility | str,
    ) -> None:
        """Instantiate a user-defined aggregate function (UDAF).

        See :py:func:`udaf` for a convenience function and argument
        descriptions.
        """
        if hasattr(accumulator, "__datafusion_aggregate_udf__"):
            self._udaf = df_internal.AggregateUDF.from_pycapsule(accumulator)
            return
        self._udaf = df_internal.AggregateUDF(
            name,
            accumulator,
            input_types,
            return_type,
            state_type,
            str(volatility),
        )

    def __repr__(self) -> str:
        """Print a string representation of the Aggregate UDF."""
        return self._udaf.__repr__()

    def __call__(self, *args: Expr) -> Expr:
        """Execute the UDAF.

        This function is not typically called by an end user. These calls will
        occur during the evaluation of the dataframe.
        """
        args_raw = [arg.expr for arg in args]
        return Expr(self._udaf.__call__(*args_raw))

    @overload
    @staticmethod
    def udaf(
        input_types: pa.DataType | list[pa.DataType],
        return_type: pa.DataType,
        state_type: list[pa.DataType],
        volatility: Volatility | str,
        name: Optional[str] = None,
    ) -> Callable[..., AggregateUDF]: ...

    @overload
    @staticmethod
    def udaf(
        accum: Callable[[], Accumulator],
        input_types: pa.DataType | list[pa.DataType],
        return_type: pa.DataType,
        state_type: list[pa.DataType],
        volatility: Volatility | str,
        name: Optional[str] = None,
    ) -> AggregateUDF: ...

    @staticmethod
    def udaf(*args: Any, **kwargs: Any):  # noqa: D417, C901
        """Create a new User-Defined Aggregate Function (UDAF).

        This class allows you to define an aggregate function that can be used in
        data aggregation or window function calls.

        Usage:
            - As a function: ``udaf(accum, input_types, return_type, state_type, volatility, name)``.
            - As a decorator: ``@udaf(input_types, return_type, state_type, volatility, name)``.
              When using ``udaf`` as a decorator, do not pass ``accum`` explicitly.

        Function example:

        If your :py:class:`Accumulator` can be instantiated with no arguments, you
        can simply pass it's type as `accum`. If you need to pass additional
        arguments to it's constructor, you can define a lambda or a factory method.
        During runtime the :py:class:`Accumulator` will be constructed for every
        instance in which this UDAF is used. The following examples are all valid::

            import pyarrow as pa
            import pyarrow.compute as pc

            class Summarize(Accumulator):
                def __init__(self, bias: float = 0.0):
                    self._sum = pa.scalar(bias)

                def state(self) -> list[pa.Scalar]:
                    return [self._sum]

                def update(self, values: pa.Array) -> None:
                    self._sum = pa.scalar(self._sum.as_py() + pc.sum(values).as_py())

                def merge(self, states: list[pa.Array]) -> None:
                    self._sum = pa.scalar(self._sum.as_py() + pc.sum(states[0]).as_py())

                def evaluate(self) -> pa.Scalar:
                    return self._sum

            def sum_bias_10() -> Summarize:
                return Summarize(10.0)

            udaf1 = udaf(Summarize, pa.float64(), pa.float64(), [pa.float64()],
                "immutable")
            udaf2 = udaf(sum_bias_10, pa.float64(), pa.float64(), [pa.float64()],
                "immutable")
            udaf3 = udaf(lambda: Summarize(20.0), pa.float64(), pa.float64(),
                [pa.float64()], "immutable")

        Decorator example:::

            @udaf(pa.float64(), pa.float64(), [pa.float64()], "immutable")
            def udf4() -> Summarize:
                return Summarize(10.0)

        Args:
            accum: The accumulator python function. Only needed when calling as a
                function. Skip this argument when using ``udaf`` as a decorator.
                If you have a Rust backed AggregateUDF within a PyCapsule, you can
                pass this parameter and ignore the rest. They will be determined
                directly from the underlying function. See the online documentation
                for more information.
            input_types: The data types of the arguments to ``accum``.
            return_type: The data type of the return value.
            state_type: The data types of the intermediate accumulation.
            volatility: See :py:class:`Volatility` for allowed values.
            name: A descriptive name for the function.

        Returns:
            A user-defined aggregate function, which can be used in either data
            aggregation or window function calls.
        """  # noqa: E501 W505

        def _function(
            accum: Callable[[], Accumulator],
            input_types: pa.DataType | list[pa.DataType],
            return_type: pa.DataType,
            state_type: list[pa.DataType],
            volatility: Volatility | str,
            name: Optional[str] = None,
        ) -> AggregateUDF:
            if not callable(accum):
                msg = "`func` must be callable."
                raise TypeError(msg)
            if not isinstance(accum(), Accumulator):
                msg = "Accumulator must implement the abstract base class Accumulator"
                raise TypeError(msg)
            if name is None:
                name = accum().__class__.__qualname__.lower()
            if isinstance(input_types, pa.DataType):
                input_types = [input_types]
            return AggregateUDF(
                name=name,
                accumulator=accum,
                input_types=input_types,
                return_type=return_type,
                state_type=state_type,
                volatility=volatility,
            )

        def _decorator(
            input_types: pa.DataType | list[pa.DataType],
            return_type: pa.DataType,
            state_type: list[pa.DataType],
            volatility: Volatility | str,
            name: Optional[str] = None,
        ) -> Callable[..., Callable[..., Expr]]:
            def decorator(accum: Callable[[], Accumulator]) -> Callable[..., Expr]:
                udaf_caller = AggregateUDF.udaf(
                    accum, input_types, return_type, state_type, volatility, name
                )

                @functools.wraps(accum)
                def wrapper(*args: Any, **kwargs: Any) -> Expr:
                    return udaf_caller(*args, **kwargs)

                return wrapper

            return decorator

        if hasattr(args[0], "__datafusion_aggregate_udf__"):
            return AggregateUDF.from_pycapsule(args[0])

        if args and callable(args[0]):
            # Case 1: Used as a function, require the first parameter to be callable
            return _function(*args, **kwargs)
        # Case 2: Used as a decorator with parameters
        return _decorator(*args, **kwargs)

    @staticmethod
    def from_pycapsule(func: AggregateUDFExportable) -> AggregateUDF:
        """Create an Aggregate UDF from AggregateUDF PyCapsule object.

        This function will instantiate a Aggregate UDF that uses a DataFusion
        AggregateUDF that is exported via the FFI bindings.
        """
        name = str(func.__class__)
        return AggregateUDF(
            name=name,
            accumulator=func,
            input_types=None,
            return_type=None,
            state_type=None,
            volatility=None,
        )


class WindowEvaluator:
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
    """  # noqa: W505, E501

    def memoize(self) -> None:
        """Perform a memoize operation to improve performance.

        When the window frame has a fixed beginning (e.g UNBOUNDED
        PRECEDING), some functions such as FIRST_VALUE and
        NTH_VALUE do not need the (unbounded) input once they have
        seen a certain amount of input.

        `memoize` is called after each input batch is processed, and
        such functions can save whatever they need
        """

    def get_range(self, idx: int, num_rows: int) -> tuple[int, int]:  # noqa: ARG002
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

    def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
        """Evaluate a window function on an entire input partition.

        This function is called once per input *partition* for window functions that
        *do not use* values from the window frame, such as
        :py:func:`~datafusion.functions.row_number`,
        :py:func:`~datafusion.functions.rank`,
        :py:func:`~datafusion.functions.dense_rank`,
        :py:func:`~datafusion.functions.percent_rank`,
        :py:func:`~datafusion.functions.cume_dist`,
        :py:func:`~datafusion.functions.lead`,
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
        """  # noqa: W505, E501

    def evaluate(
        self, values: list[pa.Array], eval_range: tuple[int, int]
    ) -> pa.Scalar:
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

    def evaluate_all_with_rank(
        self, num_rows: int, ranks_in_partition: list[tuple[int, int]]
    ) -> pa.Array:
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

    def supports_bounded_execution(self) -> bool:
        """Can the window function be incrementally computed using bounded memory?"""
        return False

    def uses_window_frame(self) -> bool:
        """Does the window function use the values from the window frame?"""
        return False

    def include_rank(self) -> bool:
        """Can this function be evaluated with (only) rank?"""
        return False


class WindowUDFExportable(Protocol):
    """Type hint for object that has __datafusion_window_udf__ PyCapsule."""

    def __datafusion_window_udf__(self) -> object: ...  # noqa: D105


class WindowUDF:
    """Class for performing window user-defined functions (UDF).

    Window UDFs operate on a partition of rows. See
    also :py:class:`ScalarUDF` for operating on a row by row basis.
    """

    def __init__(
        self,
        name: str,
        func: Callable[[], WindowEvaluator],
        input_types: list[pa.DataType],
        return_type: pa.DataType,
        volatility: Volatility | str,
    ) -> None:
        """Instantiate a user-defined window function (UDWF).

        See :py:func:`udwf` for a convenience function and argument
        descriptions.
        """
        if hasattr(func, "__datafusion_window_udf__"):
            self._udwf = df_internal.WindowUDF.from_pycapsule(func)
            return
        self._udwf = df_internal.WindowUDF(
            name, func, input_types, return_type, str(volatility)
        )

    def __repr__(self) -> str:
        """Print a string representation of the Window UDF."""
        return self._udwf.__repr__()

    def __call__(self, *args: Expr) -> Expr:
        """Execute the UDWF.

        This function is not typically called by an end user. These calls will
        occur during the evaluation of the dataframe.
        """
        args_raw = [arg.expr for arg in args]
        return Expr(self._udwf.__call__(*args_raw))

    @overload
    @staticmethod
    def udwf(
        input_types: pa.DataType | list[pa.DataType],
        return_type: pa.DataType,
        volatility: Volatility | str,
        name: Optional[str] = None,
    ) -> Callable[..., WindowUDF]: ...

    @overload
    @staticmethod
    def udwf(
        func: Callable[[], WindowEvaluator],
        input_types: pa.DataType | list[pa.DataType],
        return_type: pa.DataType,
        volatility: Volatility | str,
        name: Optional[str] = None,
    ) -> WindowUDF: ...

    @staticmethod
    def udwf(*args: Any, **kwargs: Any):  # noqa: D417
        """Create a new User-Defined Window Function (UDWF).

        This class can be used both as either a function or a decorator.

        Usage:
            - As a function: ``udwf(func, input_types, return_type, volatility, name)``.
            - As a decorator: ``@udwf(input_types, return_type, volatility, name)``.
              When using ``udwf`` as a decorator, do not pass ``func`` explicitly.

        Function example::

            import pyarrow as pa

            class BiasedNumbers(WindowEvaluator):
                def __init__(self, start: int = 0) -> None:
                    self.start = start

                def evaluate_all(self, values: list[pa.Array],
                    num_rows: int) -> pa.Array:
                    return pa.array([self.start + i for i in range(num_rows)])

            def bias_10() -> BiasedNumbers:
                return BiasedNumbers(10)

            udwf1 = udwf(BiasedNumbers, pa.int64(), pa.int64(), "immutable")
            udwf2 = udwf(bias_10, pa.int64(), pa.int64(), "immutable")
            udwf3 = udwf(lambda: BiasedNumbers(20), pa.int64(), pa.int64(), "immutable")


        Decorator example::

            @udwf(pa.int64(), pa.int64(), "immutable")
            def biased_numbers() -> BiasedNumbers:
                return BiasedNumbers(10)

        Args:
            func: Only needed when calling as a function. Skip this argument when
                using ``udwf`` as a decorator. If you have a Rust backed WindowUDF
                within a PyCapsule, you can pass this parameter and ignore the rest.
                They will be determined directly from the underlying function. See
                the online documentation for more information.
            input_types: The data types of the arguments.
            return_type: The data type of the return value.
            volatility: See :py:class:`Volatility` for allowed values.
            name: A descriptive name for the function.

        Returns:
            A user-defined window function that can be used in window function calls.
        """
        if hasattr(args[0], "__datafusion_window_udf__"):
            return WindowUDF.from_pycapsule(args[0])

        if args and callable(args[0]):
            # Case 1: Used as a function, require the first parameter to be callable
            return WindowUDF._create_window_udf(*args, **kwargs)
        # Case 2: Used as a decorator with parameters
        return WindowUDF._create_window_udf_decorator(*args, **kwargs)

    @staticmethod
    def _create_window_udf(
        func: Callable[[], WindowEvaluator],
        input_types: pa.DataType | list[pa.DataType],
        return_type: pa.DataType,
        volatility: Volatility | str,
        name: Optional[str] = None,
    ) -> WindowUDF:
        """Create a WindowUDF instance from function arguments."""
        if not callable(func):
            msg = "`func` must be callable."
            raise TypeError(msg)
        if not isinstance(func(), WindowEvaluator):
            msg = "`func` must implement the abstract base class WindowEvaluator"
            raise TypeError(msg)

        name = name or func.__qualname__.lower()
        input_types = (
            [input_types] if isinstance(input_types, pa.DataType) else input_types
        )

        return WindowUDF(name, func, input_types, return_type, volatility)

    @staticmethod
    def _get_default_name(func: Callable) -> str:
        """Get the default name for a function based on its attributes."""
        if hasattr(func, "__qualname__"):
            return func.__qualname__.lower()
        return func.__class__.__name__.lower()

    @staticmethod
    def _normalize_input_types(
        input_types: pa.DataType | list[pa.DataType],
    ) -> list[pa.DataType]:
        """Convert a single DataType to a list if needed."""
        if isinstance(input_types, pa.DataType):
            return [input_types]
        return input_types

    @staticmethod
    def _create_window_udf_decorator(
        input_types: pa.DataType | list[pa.DataType],
        return_type: pa.DataType,
        volatility: Volatility | str,
        name: Optional[str] = None,
    ) -> Callable[[Callable[[], WindowEvaluator]], Callable[..., Expr]]:
        """Create a decorator for a WindowUDF."""

        def decorator(func: Callable[[], WindowEvaluator]) -> Callable[..., Expr]:
            udwf_caller = WindowUDF._create_window_udf(
                func, input_types, return_type, volatility, name
            )

            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> Expr:
                return udwf_caller(*args, **kwargs)

            return wrapper

        return decorator

    @staticmethod
    def from_pycapsule(func: WindowUDFExportable) -> WindowUDF:
        """Create a Window UDF from WindowUDF PyCapsule object.

        This function will instantiate a Window UDF that uses a DataFusion
        WindowUDF that is exported via the FFI bindings.
        """
        name = str(func.__class__)
        return WindowUDF(
            name=name,
            func=func,
            input_types=None,
            return_type=None,
            volatility=None,
        )


class TableFunction:
    """Class for performing user-defined table functions (UDTF).

    Table functions generate new table providers based on the
    input expressions.
    """

    def __init__(
        self,
        name: str,
        func: Callable[[], any],
    ) -> None:
        """Instantiate a user-defined table function (UDTF).

        See :py:func:`udtf` for a convenience function and argument
        descriptions.
        """
        self._udtf = df_internal.TableFunction(name, func)

    def __call__(self, *args: Expr) -> Any:
        """Execute the UDTF and return a table provider."""
        args_raw = [arg.expr for arg in args]
        return self._udtf.__call__(*args_raw)

    @overload
    @staticmethod
    def udtf(
        name: str,
    ) -> Callable[..., Any]: ...

    @overload
    @staticmethod
    def udtf(
        func: Callable[[], Any],
        name: str,
    ) -> TableFunction: ...

    @staticmethod
    def udtf(*args: Any, **kwargs: Any):
        """Create a new User-Defined Table Function (UDTF)."""
        if args and callable(args[0]):
            # Case 1: Used as a function, require the first parameter to be callable
            return TableFunction._create_table_udf(*args, **kwargs)
        if args and hasattr(args[0], "__datafusion_table_function__"):
            # Case 2: We have a datafusion FFI provided function
            return TableFunction(args[1], args[0])
        # Case 3: Used as a decorator with parameters
        return TableFunction._create_table_udf_decorator(*args, **kwargs)

    @staticmethod
    def _create_table_udf(
        func: Callable[..., Any],
        name: str,
    ) -> TableFunction:
        """Create a TableFunction instance from function arguments."""
        if not callable(func):
            msg = "`func` must be callable."
            raise TypeError(msg)

        return TableFunction(name, func)

    @staticmethod
    def _create_table_udf_decorator(
        name: Optional[str] = None,
    ) -> Callable[[Callable[[], WindowEvaluator]], Callable[..., Expr]]:
        """Create a decorator for a WindowUDF."""

        def decorator(func: Callable[[], WindowEvaluator]) -> Callable[..., Expr]:
            return TableFunction._create_table_udf(func, name)

        return decorator

    def __repr__(self) -> str:
        """User printable representation."""
        return self._udtf.__repr__()


# Convenience exports so we can import instead of treating as
# variables at the package root
udf = ScalarUDF.udf
udaf = AggregateUDF.udaf
udwf = WindowUDF.udwf
udtf = TableFunction.udtf
