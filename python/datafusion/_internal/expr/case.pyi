from typing import List, Optional, Tuple
from .base import Expr


class Case:
    def expr(self) -> Optional[Expr]: ...
    
    def when_then_expr(self) -> List[Tuple[Expr, Expr]]: ...

    def else_expr(self) -> Optional[Expr]: ...