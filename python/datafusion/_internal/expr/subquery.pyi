from typing import List
from .. import LogicalPlan

class Subquery:
    def input(self) -> List[LogicalPlan]:
        ...
        
    def __name__(self) -> str:
        ...