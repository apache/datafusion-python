from ..common import DFSchema


class EmptyRelation:
    def produce_one_row(self) -> bool: ...

    def schema(self) -> DFSchema: ...

    def __name__(self) -> str: ...
    