from typing import Any, List, Optional
import pyarrow as pa

from ..common import RexType, DataTypeMap, NullTreatment
from .. import LogicalPlan
from .window import WindowFrame
from .sort_expr import SortExpr


class Expr:
    def to_variant(self) -> Any:
        ...

    def schema_name(self) -> str:
        ...
    
    def canonical_name(self) -> str:
        ...
    
    def variant_name(self) -> str:
        ...

    def __richcmp__(self, other: Expr, op: int) -> Expr: ...

    def __add__(self, rhs: Expr) -> Expr: ...

    def __sub__(self, rhs: Expr) -> Expr: ...

    def __truediv__(self, rhs: Expr) -> Expr: ...

    def __mul__(self, rhs: Expr) -> Expr: ...

    def __mod__(self, rhs: Expr) -> Expr: ...

    def __and__(self, rhs: Expr) -> Expr: ...

    def __or__(self, rhs: Expr) -> Expr: ...

    def __invert__(self) -> Expr: ...

    def __getitem__(self, key: str) -> Expr: ...

    @staticmethod
    def literal(value: Any) -> Expr: ...

    @staticmethod
    def column(value: str) -> Expr: ...

    def alias(self, name: str) -> Expr: ...

    def sort(self, ascending: bool = True, nulls_first: bool = True) -> Expr: ...

    def is_null(self) -> Expr: ...

    def is_not_null(self) -> Expr: ...

    def cast(self, to: pa.DataType) -> Expr: ...

    def between(self, low: Expr, high: Expr, negated: bool = False) -> Expr: ...

    def rex_type(self) -> RexType: ...

    def types(self) -> DataTypeMap: ...

    def python_value(self) -> Any: ...

    def rex_call_operands(self) -> List[Expr]: ...
    
    def rex_call_operator(self) -> str: ...

    def column_name(self, plan: LogicalPlan) -> str: ...

    def order_by(self, order_by: List[SortExpr]) -> ExprFuncBuilder: ...

    def filter(self, filter: Expr) -> ExprFuncBuilder: ...

    def distinct(self) -> ExprFuncBuilder: ...

    def null_treatment(self, null_treatment: NullTreatment) -> ExprFuncBuilder: ...

    def partition_by(self, partition_by: List[Expr]) ->  ExprFuncBuilder: ...

    def window_frame(self, window_frame: WindowFrame) -> ExprFuncBuilder: ...

    def over(
        self, 
        partition_by: Optional[List[Expr]] = None, 
        window_frame: Optional[WindowFrame] = None,
        order_by: Optional[List[SortExpr]] = None, 
        null_treatment: Optional[NullTreatment] = None) -> Expr:
        ...

class ExprFuncBuilder:
    def order_by(self, order_by: List[SortExpr]) -> ExprFuncBuilder: ...

    def filter(self, filter: Expr) -> ExprFuncBuilder: ...

    def distinct(self) -> ExprFuncBuilder: ...

    def null_treatment(self, null_treatment: NullTreatment) -> ExprFuncBuilder: ...

    def partition_by(self, partition_by: List[Expr]) ->  ExprFuncBuilder: ...

    def window_frame(self, window_frame: WindowFrame) -> ExprFuncBuilder: ...

    def build(self) -> Expr: ...


