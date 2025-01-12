from typing import List
from .. import LogicalPlan
from .base import Expr


class Repartition:
    def input(self) -> List[LogicalPlan]: ...
    def partitioning_scheme(self) -> Partitioning: ...
    def distribute_list(self) -> List[Expr]: ...
    def distribute_columns(self) -> str: ...
    def __name__(self) -> str: ...

class Partitioning:
    ...