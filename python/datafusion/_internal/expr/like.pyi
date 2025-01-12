from typing import Optional
from .base import Expr


class Like:
    def negated(self) -> bool: ...

    def expr(self) -> Expr: ...

    def pattern(self) -> Expr: ...

    def escape_char(self) -> Optional[str]: ...


class ILike:
    def negated(self) -> bool: ...

    def expr(self) -> Expr: ...

    def pattern(self) -> Expr: ...

    def escape_char(self) -> Optional[str]: ...

class SimilarTo:
    def negated(self) -> bool: ...

    def expr(self) -> Expr: ...

    def pattern(self) -> Expr: ...

    def escape_char(self) -> Optional[str]: ...
