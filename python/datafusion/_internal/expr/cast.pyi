from .base import Expr
from ..common import DataType


class Cast:
    def expr(self) -> Expr:  ...

    def data_type(self) -> DataType: ...


class TryCast:
    def expr(self) -> Expr:  ...

    def data_type(self) -> DataType: ...
