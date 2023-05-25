import os
import cudf
from typing import Any

from cusql.input_utils.base import BaseInputPlugin
from cusql.input_utils.convert import InputUtil


class LocationInputPlugin(BaseInputPlugin):
    """Input Plugin for everything, which can be read in from a file (on disk, remote etc.)"""

    def is_correct_input(
        self, input_item: Any, table_name: str, format: str = None, **kwargs
    ):
        return isinstance(input_item, str)

    def to_dc(
        self,
        input_item: Any,
        format: str = None,
        **kwargs,
    ):
        if not format:
            _, extension = os.path.splitext(input_item)

            format = extension.lstrip(".")
        try:
            read_function = getattr(cudf, f"read_{format}")
        except AttributeError:
            raise AttributeError(f"Can not read files of format {format}")

        return read_function(input_item, **kwargs)
