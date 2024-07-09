import datafusion._internal as df_internal
from datafusion.expr import Expr
import pyarrow
from typing import Callable


class ScalarUDF:
    def __init__(
        self,
        name: str | None,
        func: Callable,
        input_types: list[pyarrow.DataType],
        return_type: pyarrow.DataType,
        volatility: str,
    ) -> None:
        self.udf = df_internal.ScalarUDF(
            name, func, input_types, return_type, volatility
        )

    def __call__(self, *args: Expr) -> Expr:
        args = [arg.expr for arg in args]
        return Expr(self.udf.__call__(*args))


class AggregateUDF:
    def __init__(
        self,
        name: str | None,
        accumulator: Callable,
        input_types: list[pyarrow.DataType],
        return_type: pyarrow.DataType,
        state_type: list[pyarrow.DataType],
        volatility: str,
    ) -> None:
        self.udf = df_internal.AggregateUDF(
            name, accumulator, input_types, return_type, state_type, volatility
        )

    def __call__(self, *args: Expr) -> Expr:
        args = [arg.expr for arg in args]
        return Expr(self.udf.__call__(*args))
