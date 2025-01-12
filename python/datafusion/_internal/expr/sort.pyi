from typing import List, Optional

from .. import LogicalPlan
from ..common import DFSchema
from .sort_expr import SortExpr


class Sort:
    def sort_exprs(self) -> List[SortExpr]: ...

    def get_fetch_val(self) -> Optional[int]: ...

    def input(self) -> List[LogicalPlan]: ...

    def schema(self) -> DFSchema: ...