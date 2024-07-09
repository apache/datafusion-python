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

from __future__ import annotations

from ._internal import expr as expr_internal, LogicalPlan
from datafusion.common import RexType, DataTypeMap
from typing import Any
import pyarrow as pa

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
CreateMemoryTable = expr_internal.CreateMemoryTable
CreateView = expr_internal.CreateView
CrossJoin = expr_internal.CrossJoin
Distinct = expr_internal.Distinct
DropTable = expr_internal.DropTable
Exists = expr_internal.Exists
Explain = expr_internal.Explain
Extension = expr_internal.Extension
Filter = expr_internal.Filter
GroupingSet = expr_internal.GroupingSet
Join = expr_internal.Join
ILike = expr_internal.ILike
InList = expr_internal.InList
InSubquery = expr_internal.InSubquery
IsFalse = expr_internal.IsFalse
IsNotTrue = expr_internal.IsNotTrue
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
Partitioning = expr_internal.Partitioning
Placeholder = expr_internal.Placeholder
Projection = expr_internal.Projection
Repartition = expr_internal.Repartition
ScalarSubquery = expr_internal.ScalarSubquery
ScalarVariable = expr_internal.ScalarVariable
SimilarTo = expr_internal.SimilarTo
Sort = expr_internal.Sort
Subquery = expr_internal.Subquery
SubqueryAlias = expr_internal.SubqueryAlias
TableScan = expr_internal.TableScan
TryCast = expr_internal.TryCast
Union = expr_internal.Union


class Expr:
    def __init__(self, expr: expr_internal.Expr) -> None:
        self.expr = expr

    def to_variant(self) -> Any:
        return self.expr.to_variant()

    def display_name(self) -> str:
        return self.expr.display_name()

    def canonical_name(self) -> str:
        return self.expr.canonical_name()

    def variant_name(self) -> str:
        return self.expr.variant_name()

    def __richcmp__(self, other: Expr, op: int) -> Expr:
        return Expr(self.expr.__richcmp__(other, op))

    def __repr__(self) -> str:
        return self.expr.__repr__()

    def __add__(self, rhs: Expr) -> Expr:
        return Expr(self.expr.__add__(rhs.expr))

    def __sub__(self, rhs: Expr) -> Expr:
        return Expr(self.expr.__sub__(rhs.expr))

    def __truediv__(self, rhs: Expr) -> Expr:
        return Expr(self.expr.__truediv__(rhs.expr))

    def __mul__(self, rhs: Expr) -> Expr:
        return Expr(self.expr.__mul__(rhs.expr))

    def __mod__(self, rhs: Expr) -> Expr:
        return Expr(self.expr.__mod__(rhs.expr))

    def __and__(self, rhs: Expr) -> Expr:
        return Expr(self.expr.__and__(rhs.expr))

    def __or__(self, rhs: Expr) -> Expr:
        return Expr(self.expr.__or__(rhs.expr))

    def __invert__(self) -> Expr:
        return Expr(self.expr.__invert__())

    def __getitem__(self, key: str) -> Expr:
        return Expr(self.expr.__getitem__(key))

    def __eq__(self, rhs: Expr) -> Expr:
        return Expr(self.expr.__eq__(rhs.expr))

    def __ne__(self, rhs: Expr) -> Expr:
        return Expr(self.expr.__eq__(rhs.expr))

    def __ge__(self, rhs: Expr) -> Expr:
        return Expr(self.expr.__ge__(rhs.expr))

    def __gt__(self, rhs: Expr) -> Expr:
        return Expr(self.expr.__gt__(rhs.expr))

    def __le__(self, rhs: Expr) -> Expr:
        return Expr(self.expr.__le__(rhs.expr))

    def __lt__(self, rhs: Expr) -> Expr:
        return Expr(self.expr.__lt__(rhs.expr))

    @staticmethod
    def literal(value: Any) -> Expr:
        if not isinstance(value, pa.Scalar):
            value = pa.scalar(value)
        return Expr(expr_internal.Expr.literal(value))

    @staticmethod
    def column(value: str) -> Expr:
        return Expr(expr_internal.Expr.column(value))

    def alias(self, name: str) -> Expr:
        return Expr(self.expr.alias(name))

    def sort(self, ascending: bool = True, nulls_first: bool = True) -> Expr:
        return Expr(self.expr.sort(ascending=ascending, nulls_first=nulls_first))

    def is_null(self) -> Expr:
        return Expr(self.expr.is_null())

    def cast(self, to: pa.DataType[Any]) -> Expr:
        return Expr(self.expr.cast(to))

    def rex_type(self) -> RexType:
        return self.expr.rex_type()

    def types(self) -> DataTypeMap:
        return self.expr.types()

    def python_value(self) -> Any:
        return self.expr.python_value()

    def rex_call_operands(self) -> list[Expr]:
        return [Expr(e) for e in self.expr.rex_call_operands()]

    def rex_call_operator(self) -> str:
        return self.expr.rex_call_operator()

    def column_name(self, plan: LogicalPlan) -> str:
        return self.expr.column_name()


class WindowFrame:
    def __init__(
        self, units: str, start_bound: int | None, end_bound: int | None
    ) -> None:
        """
        :param units: Should be one of `rows`, `range`, or `groups`
        :param start_bound: Sets the preceeding bound. Must be >= 0. If none, this will be set to unbounded. If unit type is `groups`, this parameter must be set.
        :param end_bound: Sets the following bound. Must be >= 0. If none, this will be set to unbounded. If unit type is `groups`, this parameter must be set.
        """
        self.window_frame = expr_internal.WindowFrame(units, start_bound, end_bound)

    def get_frame_units(self) -> str:
        """
        Returns the window frame units for the bounds
        """
        return self.window_frame.get_frame_units()

    def get_lower_bound(self) -> WindowFrameBound:
        """
        Returns starting bound
        """
        return WindowFrameBound(self.window_frame.get_lower_bound())

    def get_upper_bound(self):
        """
        Returns end bound
        """
        return WindowFrameBound(self.window_frame.get_upper_bound())


class WindowFrameBound:
    def __init__(self, frame_bound: expr_internal.WindowFrameBound) -> None:
        self.frame_bound = frame_bound

    def get_offset(self) -> int | None:
        """
        Returns the offset of the window frame
        """
        return self.frame_bound.get_offset()

    def is_current_row(self) -> bool:
        """
        Returns if the frame bound is current row
        """
        return self.frame_bound.is_current_row()

    def is_following(self) -> bool:
        """
        Returns if the frame bound is following
        """
        return self.frame_bound.is_following()

    def is_preceding(self) -> bool:
        """
        Returns if the frame bound is preceding
        """
        return self.frame_bound.is_preceding()

    def is_unbounded(self) -> bool:
        """
        Returns if the frame bound is unbounded
        """
        return self.frame_bound.is_unbounded()


class CaseBuilder:
    def __init__(self, case_builder: expr_internal.CaseBuilder) -> None:
        """
        :param case_builder: Internal object. This constructor is not expected to be used by the end user. Instead use :func:`case` to construct.
        """
        self.case_builder = case_builder

    def when(self, when_expr: Expr, then_expr: Expr) -> CaseBuilder:
        return CaseBuilder(self.case_builder.when(when_expr.expr, then_expr.expr))

    def otherwise(self, else_expr: Expr) -> Expr:
        return Expr(self.case_builder.otherwise(else_expr.expr))

    def end(self) -> Expr:
        return Expr(self.case_builder.end())
