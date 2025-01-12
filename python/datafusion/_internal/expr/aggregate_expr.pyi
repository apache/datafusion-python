from typing import List
from .base import Expr


class AggregateFunction:
    def aggregate_type(self) -> str: ...

    def is_distinct(self) -> bool: ...

    def args(self) -> List[Expr]: ...
