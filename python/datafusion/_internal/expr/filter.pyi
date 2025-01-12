from typing import List
from .base import Expr
from .. import LogicalPlan
from ..common import DFSchema


class Filter:
    def predicate(self) -> Expr: ...

    def input(self) -> List[LogicalPlan]: ...

    def schema(self) -> DFSchema: ...