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

"""This module provides the user defined functions for evaluation of dataframes."""

from __future__ import annotations

import datafusion._internal as df_internal
from datafusion.expr import Expr
from typing import Callable, TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    import pyarrow

    _R = TypeVar("_R", bound=pyarrow.DataType)


class ScalarUDF:
    """Class for performing scalar user defined functions (UDF).

    Scalar UDFs operate on a row by row basis. See also ``AggregateUDF`` for operating on a group of rows.
    """

    def __init__(
        self,
        name: str | None,
        func: Callable[..., _R],
        input_types: list[pyarrow.DataType],
        return_type: _R,
        volatility: str,
    ) -> None:
        """Instantiate a scalar user defined function (UDF)."""
        self.udf = df_internal.ScalarUDF(
            name, func, input_types, return_type, volatility
        )

    def __call__(self, *args: Expr) -> Expr:
        """Execute the UDF.

        This function is not typically called by an end user. These calls will occur during the evaluation of the dataframe.
        """
        args = [arg.expr for arg in args]
        return Expr(self.udf.__call__(*args))


class AggregateUDF:
    """Class for performing scalar user defined functions (UDF).

    Aggregate UDFs operate on a group of rows and return a single value. See also ``ScalarUDF`` for operating on a row by row basis.
    """

    def __init__(
        self,
        name: str | None,
        accumulator: Callable[..., _R],
        input_types: list[pyarrow.DataType],
        return_type: _R,
        state_type: list[pyarrow.DataType],
        volatility: str,
    ) -> None:
        """Instantiate a user defined aggregate function (UDAF)."""
        self.udf = df_internal.AggregateUDF(
            name, accumulator, input_types, return_type, state_type, volatility
        )

    def __call__(self, *args: Expr) -> Expr:
        """Execute the UDAF.

        This function is not typically called by an end user. These calls will occur during the evaluation of the dataframe.
        """
        args = [arg.expr for arg in args]
        return Expr(self.udf.__call__(*args))
