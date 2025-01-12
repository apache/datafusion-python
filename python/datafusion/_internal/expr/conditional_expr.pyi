from .base import Expr


class CaseBuilder:
    def when(self, when: Expr, then: Expr) -> CaseBuilder: ...

    def otherwise(self, else_expr: Expr) -> Expr: ...

    def end(self) -> Expr: ...
    