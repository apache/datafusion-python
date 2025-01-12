from .base import Expr


class UnnestExpr:
    def expr(self) -> Expr: ...

    def __name__(self) -> str: ...