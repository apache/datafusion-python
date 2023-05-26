from typing import Any


class BaseInputPlugin:
    def is_correct_input(
        self, input_item: Any, table_name: str, format: str = None, **kwargs
    ):
        raise NotImplementedError

    def to_dc(self, input_item: Any, table_name: str, format: str = None, **kwargs):
        raise NotImplementedError
