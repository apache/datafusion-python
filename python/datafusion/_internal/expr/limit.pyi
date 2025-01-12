from typing import List
from .. import LogicalPlan
from ..common import DFSchema


class Limit:
    def input(self) -> List[LogicalPlan]: ...

    def schema(self) -> DFSchema: ...
