from .base import Expr


class SortExpr:
    def __init__(
        self,
        expr: Expr,
        asc: bool,
        nulls_first: bool,
    ) -> None: ...

    def expr(self) -> Expr: ...

    def ascending(self) -> bool: ...

    def nulls_first(self) -> bool: ...
