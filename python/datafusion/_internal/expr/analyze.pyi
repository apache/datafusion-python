from typing import List
from .. import LogicalPlan
from ..common import DFSchema


class Analyze:
    def verbose(self) -> bool: ...

    def input(self) -> List[LogicalPlan]: ...

    def schema(self) -> DFSchema: ...