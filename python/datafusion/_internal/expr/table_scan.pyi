from typing import List, Optional, Tuple
from ..common import DFSchema
from .base import Expr


class TableScan:
    def table_name(self) -> str: ...

    def fqn(self) -> Tuple[Optional[str], Optional[str], str]: ...

    def projection(self) -> List[Tuple[int, str]]: ...

    def schema(self) -> DFSchema: ...

    def filters(self) -> List[Expr]: ...

    def fetch(self) -> Optional[int]: ...

