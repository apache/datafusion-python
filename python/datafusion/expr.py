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

"""This module supports expressions, one of the core concepts in DataFusion.

See :ref:`Expressions` in the online documentation for more details.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Optional

import pyarrow as pa

try:
    from warnings import deprecated  # Python 3.13+
except ImportError:
    from typing_extensions import deprecated  # Python 3.12

from datafusion.common import DataTypeMap, NullTreatment, RexType

from ._internal import expr as expr_internal
from ._internal import functions as functions_internal

if TYPE_CHECKING:
    from datafusion.plan import LogicalPlan

# The following are imported from the internal representation. We may choose to
# give these all proper wrappers, or to simply leave as is. These were added
# in order to support passing the `test_imports` unit test.
# Tim Saucer note: It is not clear to me what the use case is for exposing
# these definitions to the end user.

Alias = expr_internal.Alias
Analyze = expr_internal.Analyze
Aggregate = expr_internal.Aggregate
AggregateFunction = expr_internal.AggregateFunction
Between = expr_internal.Between
BinaryExpr = expr_internal.BinaryExpr
Case = expr_internal.Case
Cast = expr_internal.Cast
Column = expr_internal.Column
CopyTo = expr_internal.CopyTo
CreateCatalog = expr_internal.CreateCatalog
CreateCatalogSchema = expr_internal.CreateCatalogSchema
CreateExternalTable = expr_internal.CreateExternalTable
CreateFunction = expr_internal.CreateFunction
CreateFunctionBody = expr_internal.CreateFunctionBody
CreateIndex = expr_internal.CreateIndex
CreateMemoryTable = expr_internal.CreateMemoryTable
CreateView = expr_internal.CreateView
Deallocate = expr_internal.Deallocate
DescribeTable = expr_internal.DescribeTable
Distinct = expr_internal.Distinct
DmlStatement = expr_internal.DmlStatement
DropCatalogSchema = expr_internal.DropCatalogSchema
DropFunction = expr_internal.DropFunction
DropTable = expr_internal.DropTable
DropView = expr_internal.DropView
EmptyRelation = expr_internal.EmptyRelation
Execute = expr_internal.Execute
Exists = expr_internal.Exists
Explain = expr_internal.Explain
Extension = expr_internal.Extension
FileType = expr_internal.FileType
Filter = expr_internal.Filter
GroupingSet = expr_internal.GroupingSet
Join = expr_internal.Join
ILike = expr_internal.ILike
InList = expr_internal.InList
InSubquery = expr_internal.InSubquery
IsFalse = expr_internal.IsFalse
IsNotTrue = expr_internal.IsNotTrue
IsNull = expr_internal.IsNull
IsTrue = expr_internal.IsTrue
IsUnknown = expr_internal.IsUnknown
IsNotFalse = expr_internal.IsNotFalse
IsNotNull = expr_internal.IsNotNull
IsNotUnknown = expr_internal.IsNotUnknown
JoinConstraint = expr_internal.JoinConstraint
JoinType = expr_internal.JoinType
Like = expr_internal.Like
Limit = expr_internal.Limit
Literal = expr_internal.Literal
Negative = expr_internal.Negative
Not = expr_internal.Not
OperateFunctionArg = expr_internal.OperateFunctionArg
Partitioning = expr_internal.Partitioning
Placeholder = expr_internal.Placeholder
Prepare = expr_internal.Prepare
Projection = expr_internal.Projection
RecursiveQuery = expr_internal.RecursiveQuery
Repartition = expr_internal.Repartition
ScalarSubquery = expr_internal.ScalarSubquery
ScalarVariable = expr_internal.ScalarVariable
SetVariable = expr_internal.SetVariable
SimilarTo = expr_internal.SimilarTo
Sort = expr_internal.Sort
Subquery = expr_internal.Subquery
SubqueryAlias = expr_internal.SubqueryAlias
TableScan = expr_internal.TableScan
TransactionAccessMode = expr_internal.TransactionAccessMode
TransactionConclusion = expr_internal.TransactionConclusion
TransactionEnd = expr_internal.TransactionEnd
TransactionIsolationLevel = expr_internal.TransactionIsolationLevel
TransactionStart = expr_internal.TransactionStart
TryCast = expr_internal.TryCast
Union = expr_internal.Union
Unnest = expr_internal.Unnest
UnnestExpr = expr_internal.UnnestExpr
Values = expr_internal.Values
WindowExpr = expr_internal.WindowExpr

__all__ = [
    "Aggregate",
    "AggregateFunction",
    "Alias",
    "Analyze",
    "Between",
    "BinaryExpr",
    "Case",
    "CaseBuilder",
    "Cast",
    "Column",
    "CopyTo",
    "CreateCatalog",
    "CreateCatalogSchema",
    "CreateExternalTable",
    "CreateFunction",
    "CreateFunctionBody",
    "CreateIndex",
    "CreateMemoryTable",
    "CreateView",
    "Deallocate",
    "DescribeTable",
    "Distinct",
    "DmlStatement",
    "DropCatalogSchema",
    "DropFunction",
    "DropTable",
    "DropView",
    "EmptyRelation",
    "Execute",
    "Exists",
    "Explain",
    "Expr",
    "Extension",
    "FileType",
    "Filter",
    "GroupingSet",
    "ILike",
    "InList",
    "InSubquery",
    "IsFalse",
    "IsNotFalse",
    "IsNotNull",
    "IsNotTrue",
    "IsNotUnknown",
    "IsNull",
    "IsTrue",
    "IsUnknown",
    "Join",
    "JoinConstraint",
    "JoinType",
    "Like",
    "Limit",
    "Literal",
    "Literal",
    "Negative",
    "Not",
    "OperateFunctionArg",
    "Partitioning",
    "Placeholder",
    "Prepare",
    "Projection",
    "RecursiveQuery",
    "Repartition",
    "ScalarSubquery",
    "ScalarVariable",
    "SetVariable",
    "SimilarTo",
    "Sort",
    "SortExpr",
    "Subquery",
    "SubqueryAlias",
    "TableScan",
    "TransactionAccessMode",
    "TransactionConclusion",
    "TransactionEnd",
    "TransactionIsolationLevel",
    "TransactionStart",
    "TryCast",
    "Union",
    "Unnest",
    "UnnestExpr",
    "Values",
    "Window",
    "WindowExpr",
    "WindowFrame",
    "WindowFrameBound",
]


def expr_list_to_raw_expr_list(
    expr_list: Optional[list[Expr]],
) -> Optional[list[expr_internal.Expr]]:
    """Helper function to convert an optional list to raw expressions."""
    return [e.expr for e in expr_list] if expr_list is not None else None


def sort_or_default(e: Expr | SortExpr) -> expr_internal.SortExpr:
    """Helper function to return a default Sort if an Expr is provided."""
    if isinstance(e, SortExpr):
        return e.raw_sort
    return SortExpr(e, ascending=True, nulls_first=True).raw_sort


def sort_list_to_raw_sort_list(
    sort_list: Optional[list[Expr | SortExpr]],
) -> Optional[list[expr_internal.SortExpr]]:
    """Helper function to return an optional sort list to raw variant."""
    return [sort_or_default(e) for e in sort_list] if sort_list is not None else None


class Expr:
    """Expression object.

    Expressions are one of the core concepts in DataFusion. See
    :ref:`Expressions` in the online documentation for more information.
    """

    def __init__(self, expr: expr_internal.RawExpr) -> None:
        """This constructor should not be called by the end user."""
        self.expr = expr

    def to_variant(self) -> Any:
        """Convert this expression into a python object if possible."""
        return self.expr.to_variant()

    @deprecated(
        "display_name() is deprecated. Use :py:meth:`~Expr.schema_name` instead"
    )
    def display_name(self) -> str:
        """Returns the name of this expression as it should appear in a schema.

        This name will not include any CAST expressions.
        """
        return self.schema_name()

    def schema_name(self) -> str:
        """Returns the name of this expression as it should appear in a schema.

        This name will not include any CAST expressions.
        """
        return self.expr.schema_name()

    def canonical_name(self) -> str:
        """Returns a complete string representation of this expression."""
        return self.expr.canonical_name()

    def variant_name(self) -> str:
        """Returns the name of the Expr variant.

        Ex: ``IsNotNull``, ``Literal``, ``BinaryExpr``, etc
        """
        return self.expr.variant_name()

    def __richcmp__(self, other: Expr, op: int) -> Expr:
        """Comparison operator."""
        return Expr(self.expr.__richcmp__(other.expr, op))

    def __repr__(self) -> str:
        """Generate a string representation of this expression."""
        return self.expr.__repr__()

    def __add__(self, rhs: Any) -> Expr:
        """Addition operator.

        Accepts either an expression or any valid PyArrow scalar literal value.
        """
        if not isinstance(rhs, Expr):
            rhs = Expr.literal(rhs)
        return Expr(self.expr.__add__(rhs.expr))

    def __sub__(self, rhs: Any) -> Expr:
        """Subtraction operator.

        Accepts either an expression or any valid PyArrow scalar literal value.
        """
        if not isinstance(rhs, Expr):
            rhs = Expr.literal(rhs)
        return Expr(self.expr.__sub__(rhs.expr))

    def __truediv__(self, rhs: Any) -> Expr:
        """Division operator.

        Accepts either an expression or any valid PyArrow scalar literal value.
        """
        if not isinstance(rhs, Expr):
            rhs = Expr.literal(rhs)
        return Expr(self.expr.__truediv__(rhs.expr))

    def __mul__(self, rhs: Any) -> Expr:
        """Multiplication operator.

        Accepts either an expression or any valid PyArrow scalar literal value.
        """
        if not isinstance(rhs, Expr):
            rhs = Expr.literal(rhs)
        return Expr(self.expr.__mul__(rhs.expr))

    def __mod__(self, rhs: Any) -> Expr:
        """Modulo operator (%).

        Accepts either an expression or any valid PyArrow scalar literal value.
        """
        if not isinstance(rhs, Expr):
            rhs = Expr.literal(rhs)
        return Expr(self.expr.__mod__(rhs.expr))

    def __and__(self, rhs: Expr) -> Expr:
        """Logical AND."""
        if not isinstance(rhs, Expr):
            rhs = Expr.literal(rhs)
        return Expr(self.expr.__and__(rhs.expr))

    def __or__(self, rhs: Expr) -> Expr:
        """Logical OR."""
        if not isinstance(rhs, Expr):
            rhs = Expr.literal(rhs)
        return Expr(self.expr.__or__(rhs.expr))

    def __invert__(self) -> Expr:
        """Binary not (~)."""
        return Expr(self.expr.__invert__())

    def __getitem__(self, key: str | int) -> Expr:
        """Retrieve sub-object.

        If ``key`` is a string, returns the subfield of the struct.
        If ``key`` is an integer, retrieves the element in the array. Note that the
        element index begins at ``0``, unlike `array_element` which begins at ``1``.
        """
        if isinstance(key, int):
            return Expr(
                functions_internal.array_element(self.expr, Expr.literal(key + 1).expr)
            )
        return Expr(self.expr.__getitem__(key))

    def __eq__(self, rhs: object) -> Expr:
        """Equal to.

        Accepts either an expression or any valid PyArrow scalar literal value.
        """
        if not isinstance(rhs, Expr):
            rhs = Expr.literal(rhs)
        return Expr(self.expr.__eq__(rhs.expr))

    def __ne__(self, rhs: object) -> Expr:
        """Not equal to.

        Accepts either an expression or any valid PyArrow scalar literal value.
        """
        if not isinstance(rhs, Expr):
            rhs = Expr.literal(rhs)
        return Expr(self.expr.__ne__(rhs.expr))

    def __ge__(self, rhs: Any) -> Expr:
        """Greater than or equal to.

        Accepts either an expression or any valid PyArrow scalar literal value.
        """
        if not isinstance(rhs, Expr):
            rhs = Expr.literal(rhs)
        return Expr(self.expr.__ge__(rhs.expr))

    def __gt__(self, rhs: Any) -> Expr:
        """Greater than.

        Accepts either an expression or any valid PyArrow scalar literal value.
        """
        if not isinstance(rhs, Expr):
            rhs = Expr.literal(rhs)
        return Expr(self.expr.__gt__(rhs.expr))

    def __le__(self, rhs: Any) -> Expr:
        """Less than or equal to.

        Accepts either an expression or any valid PyArrow scalar literal value.
        """
        if not isinstance(rhs, Expr):
            rhs = Expr.literal(rhs)
        return Expr(self.expr.__le__(rhs.expr))

    def __lt__(self, rhs: Any) -> Expr:
        """Less than.

        Accepts either an expression or any valid PyArrow scalar literal value.
        """
        if not isinstance(rhs, Expr):
            rhs = Expr.literal(rhs)
        return Expr(self.expr.__lt__(rhs.expr))

    __radd__ = __add__
    __rand__ = __and__
    __rmod__ = __mod__
    __rmul__ = __mul__
    __ror__ = __or__
    __rsub__ = __sub__
    __rtruediv__ = __truediv__

    @staticmethod
    def literal(value: Any) -> Expr:
        """Creates a new expression representing a scalar value.

        ``value`` must be a valid PyArrow scalar value or easily castable to one.
        """
        if isinstance(value, str):
            value = pa.scalar(value, type=pa.string_view())
        if not isinstance(value, pa.Scalar):
            value = pa.scalar(value)
        return Expr(expr_internal.RawExpr.literal(value))

    @staticmethod
    def literal_with_metadata(value: Any, metadata: dict[str, str]) -> Expr:
        """Creates a new expression representing a scalar value with metadata.

        Args:
            value: A valid PyArrow scalar value or easily castable to one.
            metadata: Metadata to attach to the expression.
        """
        if isinstance(value, str):
            value = pa.scalar(value, type=pa.string_view())
        value = value if isinstance(value, pa.Scalar) else pa.scalar(value)

        return Expr(expr_internal.RawExpr.literal_with_metadata(value, metadata))

    @staticmethod
    def string_literal(value: str) -> Expr:
        """Creates a new expression representing a UTF8 literal value.

        It is different from `literal` because it is pa.string() instead of
        pa.string_view()

        This is needed for cases where DataFusion is expecting a UTF8 instead of
        UTF8View literal, like in:
        https://github.com/apache/datafusion/blob/86740bfd3d9831d6b7c1d0e1bf4a21d91598a0ac/datafusion/functions/src/core/arrow_cast.rs#L179
        """
        if isinstance(value, str):
            value = pa.scalar(value, type=pa.string())
            return Expr(expr_internal.RawExpr.literal(value))
        return Expr.literal(value)

    @staticmethod
    def column(value: str) -> Expr:
        """Creates a new expression representing a column."""
        return Expr(expr_internal.RawExpr.column(value))

    def alias(self, name: str, metadata: Optional[dict[str, str]] = None) -> Expr:
        """Assign a name to the expression.

        Args:
            name: The name to assign to the expression.
            metadata: Optional metadata to attach to the expression.

        Returns:
            A new expression with the assigned name.
        """
        return Expr(self.expr.alias(name, metadata))

    def sort(self, ascending: bool = True, nulls_first: bool = True) -> SortExpr:
        """Creates a sort :py:class:`Expr` from an existing :py:class:`Expr`.

        Args:
            ascending: If true, sort in ascending order.
            nulls_first: Return null values first.
        """
        return SortExpr(self, ascending=ascending, nulls_first=nulls_first)

    def is_null(self) -> Expr:
        """Returns ``True`` if this expression is null."""
        return Expr(self.expr.is_null())

    def is_not_null(self) -> Expr:
        """Returns ``True`` if this expression is not null."""
        return Expr(self.expr.is_not_null())

    def fill_nan(self, value: Any | Expr | None = None) -> Expr:
        """Fill NaN values with a provided value."""
        if not isinstance(value, Expr):
            value = Expr.literal(value)
        return Expr(functions_internal.nanvl(self.expr, value.expr))

    def fill_null(self, value: Any | Expr | None = None) -> Expr:
        """Fill NULL values with a provided value."""
        if not isinstance(value, Expr):
            value = Expr.literal(value)
        return Expr(functions_internal.nvl(self.expr, value.expr))

    _to_pyarrow_types: ClassVar[dict[type, pa.DataType]] = {
        float: pa.float64(),
        int: pa.int64(),
        str: pa.string(),
        bool: pa.bool_(),
    }

    def cast(self, to: pa.DataType[Any] | type[float | int | str | bool]) -> Expr:
        """Cast to a new data type."""
        if not isinstance(to, pa.DataType):
            try:
                to = self._to_pyarrow_types[to]
            except KeyError as err:
                error_msg = "Expected instance of pyarrow.DataType or builtins.type"
                raise TypeError(error_msg) from err

        return Expr(self.expr.cast(to))

    def between(self, low: Any, high: Any, negated: bool = False) -> Expr:
        """Returns ``True`` if this expression is between a given range.

        Args:
            low: lower bound of the range (inclusive).
            high: higher bound of the range (inclusive).
            negated: negates whether the expression is between a given range
        """
        if not isinstance(low, Expr):
            low = Expr.literal(low)

        if not isinstance(high, Expr):
            high = Expr.literal(high)

        return Expr(self.expr.between(low.expr, high.expr, negated=negated))

    def rex_type(self) -> RexType:
        """Return the Rex Type of this expression.

        A Rex (Row Expression) specifies a single row of data.That specification
        could include user defined functions or types. RexType identifies the
        row as one of the possible valid ``RexType``.
        """
        return self.expr.rex_type()

    def types(self) -> DataTypeMap:
        """Return the ``DataTypeMap``.

        Returns:
            DataTypeMap which represents the PythonType, Arrow DataType, and
            SqlType Enum which this expression represents.
        """
        return self.expr.types()

    def python_value(self) -> Any:
        """Extracts the Expr value into a PyObject.

        This is only valid for literal expressions.

        Returns:
            Python object representing literal value of the expression.
        """
        return self.expr.python_value()

    def rex_call_operands(self) -> list[Expr]:
        """Return the operands of the expression based on it's variant type.

        Row expressions, Rex(s), operate on the concept of operands. Different
        variants of Expressions, Expr(s), store those operands in different
        datastructures. This function examines the Expr variant and returns
        the operands to the calling logic.
        """
        return [Expr(e) for e in self.expr.rex_call_operands()]

    def rex_call_operator(self) -> str:
        """Extracts the operator associated with a row expression type call."""
        return self.expr.rex_call_operator()

    def column_name(self, plan: LogicalPlan) -> str:
        """Compute the output column name based on the provided logical plan."""
        return self.expr.column_name(plan._raw_plan)

    def order_by(self, *exprs: Expr | SortExpr) -> ExprFuncBuilder:
        """Set the ordering for a window or aggregate function.

        This function will create an :py:class:`ExprFuncBuilder` that can be used to
        set parameters for either window or aggregate functions. If used on any other
        type of expression, an error will be generated when ``build()`` is called.
        """
        return ExprFuncBuilder(self.expr.order_by([sort_or_default(e) for e in exprs]))

    def filter(self, filter: Expr) -> ExprFuncBuilder:
        """Filter an aggregate function.

        This function will create an :py:class:`ExprFuncBuilder` that can be used to
        set parameters for either window or aggregate functions. If used on any other
        type of expression, an error will be generated when ``build()`` is called.
        """
        return ExprFuncBuilder(self.expr.filter(filter.expr))

    def distinct(self) -> ExprFuncBuilder:
        """Only evaluate distinct values for an aggregate function.

        This function will create an :py:class:`ExprFuncBuilder` that can be used to
        set parameters for either window or aggregate functions. If used on any other
        type of expression, an error will be generated when ``build()`` is called.
        """
        return ExprFuncBuilder(self.expr.distinct())

    def null_treatment(self, null_treatment: NullTreatment) -> ExprFuncBuilder:
        """Set the treatment for ``null`` values for a window or aggregate function.

        This function will create an :py:class:`ExprFuncBuilder` that can be used to
        set parameters for either window or aggregate functions. If used on any other
        type of expression, an error will be generated when ``build()`` is called.
        """
        return ExprFuncBuilder(self.expr.null_treatment(null_treatment.value))

    def partition_by(self, *partition_by: Expr) -> ExprFuncBuilder:
        """Set the partitioning for a window function.

        This function will create an :py:class:`ExprFuncBuilder` that can be used to
        set parameters for either window or aggregate functions. If used on any other
        type of expression, an error will be generated when ``build()`` is called.
        """
        return ExprFuncBuilder(self.expr.partition_by([e.expr for e in partition_by]))

    def window_frame(self, window_frame: WindowFrame) -> ExprFuncBuilder:
        """Set the frame fora  window function.

        This function will create an :py:class:`ExprFuncBuilder` that can be used to
        set parameters for either window or aggregate functions. If used on any other
        type of expression, an error will be generated when ``build()`` is called.
        """
        return ExprFuncBuilder(self.expr.window_frame(window_frame.window_frame))

    def over(self, window: Window) -> Expr:
        """Turn an aggregate function into a window function.

        This function turns any aggregate function into a window function. With the
        exception of ``partition_by``, how each of the parameters is used is determined
        by the underlying aggregate function.

        Args:
            window: Window definition
        """
        partition_by_raw = expr_list_to_raw_expr_list(window._partition_by)
        order_by_raw = sort_list_to_raw_sort_list(window._order_by)
        window_frame_raw = (
            window._window_frame.window_frame
            if window._window_frame is not None
            else None
        )
        null_treatment_raw = (
            window._null_treatment.value if window._null_treatment is not None else None
        )

        return Expr(
            self.expr.over(
                partition_by=partition_by_raw,
                order_by=order_by_raw,
                window_frame=window_frame_raw,
                null_treatment=null_treatment_raw,
            )
        )

    def asin(self) -> Expr:
        """Returns the arc sine or inverse sine of a number."""
        from . import functions as F

        return F.asin(self)

    def array_pop_back(self) -> Expr:
        """Returns the array without the last element."""
        from . import functions as F

        return F.array_pop_back(self)

    def reverse(self) -> Expr:
        """Reverse the string argument."""
        from . import functions as F

        return F.reverse(self)

    def bit_length(self) -> Expr:
        """Returns the number of bits in the string argument."""
        from . import functions as F

        return F.bit_length(self)

    def array_length(self) -> Expr:
        """Returns the length of the array."""
        from . import functions as F

        return F.array_length(self)

    def array_ndims(self) -> Expr:
        """Returns the number of dimensions of the array."""
        from . import functions as F

        return F.array_ndims(self)

    def to_hex(self) -> Expr:
        """Converts an integer to a hexadecimal string."""
        from . import functions as F

        return F.to_hex(self)

    def array_dims(self) -> Expr:
        """Returns an array of the array's dimensions."""
        from . import functions as F

        return F.array_dims(self)

    def from_unixtime(self) -> Expr:
        """Converts an integer to RFC3339 timestamp format string."""
        from . import functions as F

        return F.from_unixtime(self)

    def array_empty(self) -> Expr:
        """Returns a boolean indicating whether the array is empty."""
        from . import functions as F

        return F.array_empty(self)

    def sin(self) -> Expr:
        """Returns the sine of the argument."""
        from . import functions as F

        return F.sin(self)

    def log10(self) -> Expr:
        """Base 10 logarithm of the argument."""
        from . import functions as F

        return F.log10(self)

    def initcap(self) -> Expr:
        """Set the initial letter of each word to capital.

        Converts the first letter of each word in ``string`` to uppercase and the
        remaining characters to lowercase.
        """
        from . import functions as F

        return F.initcap(self)

    def list_distinct(self) -> Expr:
        """Returns distinct values from the array after removing duplicates.

        This is an alias for :py:func:`array_distinct`.
        """
        from . import functions as F

        return F.list_distinct(self)

    def iszero(self) -> Expr:
        """Returns true if a given number is +0.0 or -0.0 otherwise returns false."""
        from . import functions as F

        return F.iszero(self)

    def array_distinct(self) -> Expr:
        """Returns distinct values from the array after removing duplicates."""
        from . import functions as F

        return F.array_distinct(self)

    def arrow_typeof(self) -> Expr:
        """Returns the Arrow type of the expression."""
        from . import functions as F

        return F.arrow_typeof(self)

    def length(self) -> Expr:
        """The number of characters in the ``string``."""
        from . import functions as F

        return F.length(self)

    def lower(self) -> Expr:
        """Converts a string to lowercase."""
        from . import functions as F

        return F.lower(self)

    def acos(self) -> Expr:
        """Returns the arc cosine or inverse cosine of a number.

        Returns:
        --------
        Expr
            A new expression representing the arc cosine of the input expression.
        """
        from . import functions as F

        return F.acos(self)

    def ascii(self) -> Expr:
        """Returns the numeric code of the first character of the argument."""
        from . import functions as F

        return F.ascii(self)

    def sha384(self) -> Expr:
        """Computes the SHA-384 hash of a binary string."""
        from . import functions as F

        return F.sha384(self)

    def isnan(self) -> Expr:
        """Returns true if a given number is +NaN or -NaN otherwise returns false."""
        from . import functions as F

        return F.isnan(self)

    def degrees(self) -> Expr:
        """Converts the argument from radians to degrees."""
        from . import functions as F

        return F.degrees(self)

    def cardinality(self) -> Expr:
        """Returns the total number of elements in the array."""
        from . import functions as F

        return F.cardinality(self)

    def sha224(self) -> Expr:
        """Computes the SHA-224 hash of a binary string."""
        from . import functions as F

        return F.sha224(self)

    def asinh(self) -> Expr:
        """Returns inverse hyperbolic sine."""
        from . import functions as F

        return F.asinh(self)

    def flatten(self) -> Expr:
        """Flattens an array of arrays into a single array."""
        from . import functions as F

        return F.flatten(self)

    def exp(self) -> Expr:
        """Returns the exponential of the argument."""
        from . import functions as F

        return F.exp(self)

    def abs(self) -> Expr:
        """Return the absolute value of a given number.

        Returns:
        --------
        Expr
            A new expression representing the absolute value of the input expression.
        """
        from . import functions as F

        return F.abs(self)

    def btrim(self) -> Expr:
        """Removes all characters, spaces by default, from both sides of a string."""
        from . import functions as F

        return F.btrim(self)

    def md5(self) -> Expr:
        """Computes an MD5 128-bit checksum for a string expression."""
        from . import functions as F

        return F.md5(self)

    def octet_length(self) -> Expr:
        """Returns the number of bytes of a string."""
        from . import functions as F

        return F.octet_length(self)

    def cosh(self) -> Expr:
        """Returns the hyperbolic cosine of the argument."""
        from . import functions as F

        return F.cosh(self)

    def radians(self) -> Expr:
        """Converts the argument from degrees to radians."""
        from . import functions as F

        return F.radians(self)

    def sqrt(self) -> Expr:
        """Returns the square root of the argument."""
        from . import functions as F

        return F.sqrt(self)

    def character_length(self) -> Expr:
        """Returns the number of characters in the argument."""
        from . import functions as F

        return F.character_length(self)

    def tanh(self) -> Expr:
        """Returns the hyperbolic tangent of the argument."""
        from . import functions as F

        return F.tanh(self)

    def atan(self) -> Expr:
        """Returns inverse tangent of a number."""
        from . import functions as F

        return F.atan(self)

    def rtrim(self) -> Expr:
        """Removes all characters, spaces by default, from the end of a string."""
        from . import functions as F

        return F.rtrim(self)

    def atanh(self) -> Expr:
        """Returns inverse hyperbolic tangent."""
        from . import functions as F

        return F.atanh(self)

    def list_dims(self) -> Expr:
        """Returns an array of the array's dimensions.

        This is an alias for :py:func:`array_dims`.
        """
        from . import functions as F

        return F.list_dims(self)

    def sha256(self) -> Expr:
        """Computes the SHA-256 hash of a binary string."""
        from . import functions as F

        return F.sha256(self)

    def factorial(self) -> Expr:
        """Returns the factorial of the argument."""
        from . import functions as F

        return F.factorial(self)

    def acosh(self) -> Expr:
        """Returns inverse hyperbolic cosine."""
        from . import functions as F

        return F.acosh(self)

    def floor(self) -> Expr:
        """Returns the nearest integer less than or equal to the argument."""
        from . import functions as F

        return F.floor(self)

    def ceil(self) -> Expr:
        """Returns the nearest integer greater than or equal to argument."""
        from . import functions as F

        return F.ceil(self)

    def list_length(self) -> Expr:
        """Returns the length of the array.

        This is an alias for :py:func:`array_length`.
        """
        from . import functions as F

        return F.list_length(self)

    def upper(self) -> Expr:
        """Converts a string to uppercase."""
        from . import functions as F

        return F.upper(self)

    def chr(self) -> Expr:
        """Converts the Unicode code point to a UTF8 character."""
        from . import functions as F

        return F.chr(self)

    def ln(self) -> Expr:
        """Returns the natural logarithm (base e) of the argument."""
        from . import functions as F

        return F.ln(self)

    def tan(self) -> Expr:
        """Returns the tangent of the argument."""
        from . import functions as F

        return F.tan(self)

    def array_pop_front(self) -> Expr:
        """Returns the array without the first element."""
        from . import functions as F

        return F.array_pop_front(self)

    def cbrt(self) -> Expr:
        """Returns the cube root of a number."""
        from . import functions as F

        return F.cbrt(self)

    def sha512(self) -> Expr:
        """Computes the SHA-512 hash of a binary string."""
        from . import functions as F

        return F.sha512(self)

    def char_length(self) -> Expr:
        """The number of characters in the ``string``."""
        from . import functions as F

        return F.char_length(self)

    def list_ndims(self) -> Expr:
        """Returns the number of dimensions of the array.

        This is an alias for :py:func:`array_ndims`.
        """
        from . import functions as F

        return F.list_ndims(self)

    def trim(self) -> Expr:
        """Removes all characters, spaces by default, from both sides of a string."""
        from . import functions as F

        return F.trim(self)

    def cos(self) -> Expr:
        """Returns the cosine of the argument."""
        from . import functions as F

        return F.cos(self)

    def sinh(self) -> Expr:
        """Returns the hyperbolic sine of the argument."""
        from . import functions as F

        return F.sinh(self)

    def empty(self) -> Expr:
        """This is an alias for :py:func:`array_empty`."""
        from . import functions as F

        return F.empty(self)

    def ltrim(self) -> Expr:
        """Removes all characters, spaces by default, from the beginning of a string."""
        from . import functions as F

        return F.ltrim(self)

    def signum(self) -> Expr:
        """Returns the sign of the argument (-1, 0, +1)."""
        from . import functions as F

        return F.signum(self)

    def log2(self) -> Expr:
        """Base 2 logarithm of the argument."""
        from . import functions as F

        return F.log2(self)

    def cot(self) -> Expr:
        """Returns the cotangent of the argument."""
        from . import functions as F

        return F.cot(self)


class ExprFuncBuilder:
    def __init__(self, builder: expr_internal.ExprFuncBuilder) -> None:
        self.builder = builder

    def order_by(self, *exprs: Expr) -> ExprFuncBuilder:
        """Set the ordering for a window or aggregate function.

        Values given in ``exprs`` must be sort expressions. You can convert any other
        expression to a sort expression using `.sort()`.
        """
        return ExprFuncBuilder(
            self.builder.order_by([sort_or_default(e) for e in exprs])
        )

    def filter(self, filter: Expr) -> ExprFuncBuilder:
        """Filter values during aggregation."""
        return ExprFuncBuilder(self.builder.filter(filter.expr))

    def distinct(self) -> ExprFuncBuilder:
        """Only evaluate distinct values during aggregation."""
        return ExprFuncBuilder(self.builder.distinct())

    def null_treatment(self, null_treatment: NullTreatment) -> ExprFuncBuilder:
        """Set how nulls are treated for either window or aggregate functions."""
        return ExprFuncBuilder(self.builder.null_treatment(null_treatment.value))

    def partition_by(self, *partition_by: Expr) -> ExprFuncBuilder:
        """Set partitioning for window functions."""
        return ExprFuncBuilder(
            self.builder.partition_by([e.expr for e in partition_by])
        )

    def window_frame(self, window_frame: WindowFrame) -> ExprFuncBuilder:
        """Set window frame for window functions."""
        return ExprFuncBuilder(self.builder.window_frame(window_frame.window_frame))

    def build(self) -> Expr:
        """Create an expression from a Function Builder."""
        return Expr(self.builder.build())


class Window:
    """Define reusable window parameters."""

    def __init__(
        self,
        partition_by: Optional[list[Expr]] = None,
        window_frame: Optional[WindowFrame] = None,
        order_by: Optional[list[SortExpr | Expr]] = None,
        null_treatment: Optional[NullTreatment] = None,
    ) -> None:
        """Construct a window definition.

        Args:
            partition_by: Partitions for window operation
            window_frame: Define the start and end bounds of the window frame
            order_by: Set ordering
            null_treatment: Indicate how nulls are to be treated
        """
        self._partition_by = partition_by
        self._window_frame = window_frame
        self._order_by = order_by
        self._null_treatment = null_treatment


class WindowFrame:
    """Defines a window frame for performing window operations."""

    def __init__(
        self, units: str, start_bound: Optional[Any], end_bound: Optional[Any]
    ) -> None:
        """Construct a window frame using the given parameters.

        Args:
            units: Should be one of ``rows``, ``range``, or ``groups``.
            start_bound: Sets the preceding bound. Must be >= 0. If none, this
                will be set to unbounded. If unit type is ``groups``, this
                parameter must be set.
            end_bound: Sets the following bound. Must be >= 0. If none, this
                will be set to unbounded. If unit type is ``groups``, this
                parameter must be set.
        """
        if not isinstance(start_bound, pa.Scalar) and start_bound is not None:
            start_bound = pa.scalar(start_bound)
            if units in ("rows", "groups"):
                start_bound = start_bound.cast(pa.uint64())
        if not isinstance(end_bound, pa.Scalar) and end_bound is not None:
            end_bound = pa.scalar(end_bound)
            if units in ("rows", "groups"):
                end_bound = end_bound.cast(pa.uint64())
        self.window_frame = expr_internal.WindowFrame(units, start_bound, end_bound)

    def __repr__(self) -> str:
        """Print a string representation of the window frame."""
        return self.window_frame.__repr__()

    def get_frame_units(self) -> str:
        """Returns the window frame units for the bounds."""
        return self.window_frame.get_frame_units()

    def get_lower_bound(self) -> WindowFrameBound:
        """Returns starting bound."""
        return WindowFrameBound(self.window_frame.get_lower_bound())

    def get_upper_bound(self) -> WindowFrameBound:
        """Returns end bound."""
        return WindowFrameBound(self.window_frame.get_upper_bound())


class WindowFrameBound:
    """Defines a single window frame bound.

    :py:class:`WindowFrame` typically requires a start and end bound.
    """

    def __init__(self, frame_bound: expr_internal.WindowFrameBound) -> None:
        """Constructs a window frame bound."""
        self.frame_bound = frame_bound

    def get_offset(self) -> int | None:
        """Returns the offset of the window frame."""
        return self.frame_bound.get_offset()

    def is_current_row(self) -> bool:
        """Returns if the frame bound is current row."""
        return self.frame_bound.is_current_row()

    def is_following(self) -> bool:
        """Returns if the frame bound is following."""
        return self.frame_bound.is_following()

    def is_preceding(self) -> bool:
        """Returns if the frame bound is preceding."""
        return self.frame_bound.is_preceding()

    def is_unbounded(self) -> bool:
        """Returns if the frame bound is unbounded."""
        return self.frame_bound.is_unbounded()


class CaseBuilder:
    """Builder class for constructing case statements.

    An example usage would be as follows::

        import datafusion.functions as f
        from datafusion import lit, col
        df.select(
            f.case(col("column_a")
            .when(lit(1), lit("One"))
            .when(lit(2), lit("Two"))
            .otherwise(lit("Unknown"))
        )
    """

    def __init__(self, case_builder: expr_internal.CaseBuilder) -> None:
        """Constructs a case builder.

        This is not typically called by the end user directly. See
        :py:func:`datafusion.functions.case` instead.
        """
        self.case_builder = case_builder

    def when(self, when_expr: Expr, then_expr: Expr) -> CaseBuilder:
        """Add a case to match against."""
        return CaseBuilder(self.case_builder.when(when_expr.expr, then_expr.expr))

    def otherwise(self, else_expr: Expr) -> Expr:
        """Set a default value for the case statement."""
        return Expr(self.case_builder.otherwise(else_expr.expr))

    def end(self) -> Expr:
        """Finish building a case statement.

        Any non-matching cases will end in a `null` value.
        """
        return Expr(self.case_builder.end())


class SortExpr:
    """Used to specify sorting on either a DataFrame or function."""

    def __init__(self, expr: Expr, ascending: bool, nulls_first: bool) -> None:
        """This constructor should not be called by the end user."""
        self.raw_sort = expr_internal.SortExpr(expr.expr, ascending, nulls_first)

    def expr(self) -> Expr:
        """Return the raw expr backing the SortExpr."""
        return Expr(self.raw_sort.expr())

    def ascending(self) -> bool:
        """Return ascending property."""
        return self.raw_sort.ascending()

    def nulls_first(self) -> bool:
        """Return nulls_first property."""
        return self.raw_sort.nulls_first()

    def __repr__(self) -> str:
        """Generate a string representation of this expression."""
        return self.raw_sort.__repr__()
