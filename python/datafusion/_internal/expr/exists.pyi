from .subquery import Subquery


class Exists:
    def subquery(self) -> Subquery: ...

    def negated(self) -> bool: ...