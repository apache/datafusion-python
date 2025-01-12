from typing import List
from .. import LogicalPlan
from ..common import DFSchema


class Unnest:
    def input(self) -> List[LogicalPlan]: ...

    def schema(self) -> DFSchema: ...

    def __name__(self) -> str: ...