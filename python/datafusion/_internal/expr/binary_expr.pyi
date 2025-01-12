from .base import Expr


class BinaryExpr:
    def left(self) -> Expr: ...

    def right(self) -> Expr: ...

    def on(self) -> str: ...
    