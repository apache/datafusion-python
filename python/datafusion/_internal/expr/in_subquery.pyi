from .base import Expr
from .subquery import Subquery


class InSubquery:
    def expr(self) -> Expr: ...

    def subquery(self) -> Subquery: ...

    def negated(self) -> bool: ...
    