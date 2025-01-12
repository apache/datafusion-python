from typing import List
from .. import LogicalPlan


class Distinct:
    def input(self) -> List[LogicalPlan]: ...

    def __name__(self) -> str: ...
    