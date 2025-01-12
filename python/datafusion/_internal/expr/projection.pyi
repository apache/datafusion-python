from typing import List
from .base import Expr
from .. import LogicalPlan
from ..common import DFSchema


class Projection:
    def projections(self) -> List[Expr]: ...

    def input(self) -> List[LogicalPlan]: ...

    def schema(self) -> DFSchema: ...

    def __name__(self) -> str: ...
    