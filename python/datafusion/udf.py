# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import datafusion._internal as df_internal
from datafusion.expr import Expr
from typing import Callable, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow


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
