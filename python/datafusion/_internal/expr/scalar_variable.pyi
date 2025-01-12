from typing import List
from ..common import DataType


class ScalarVariable:
    def data_type(self) -> DataType:
        ...

    def variables(self) -> List[str]:
        ...
