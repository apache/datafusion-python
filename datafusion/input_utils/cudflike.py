import cudf

from datafusion.input_utils.base import BaseInputPlugin


class CudfLikeInputPlugin(BaseInputPlugin):
    """Input Plugin for Pandas Like DataFrames, which get converted to dask DataFrames"""

    def is_correct_input(
        self, input_item, table_name: str, format: str = None, **kwargs
    ):
        return hasattr(input_item, "__dataframe__")

    def to_dc(
        self,
        input_item,
        table_name: str,
        format: str = None,
        gpu: bool = False,
        **kwargs,
    ):
        return input_item
