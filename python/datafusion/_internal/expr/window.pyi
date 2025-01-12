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

from typing import Any, List, Optional
from ..common import DFSchema
from .base import Expr
from .sort_expr import SortExpr


class WindowExpr:
    def schema(self) -> DFSchema: ...

    def get_window_expr(self) -> List[Expr]: ...

    def get_sort_exprs(self, expr: Expr) -> List[SortExpr]: ...

    def get_partition_exprs(self, expr: Expr) -> List[Expr]: ...

    def get_args(self, expr: Expr) -> List[Expr]: ...

    def window_func_name(self, expr: Expr) -> str: ...

    def get_frame(self, expr: Expr) -> Optional[WindowFrame]: ...


class WindowFrame:
    def __init__(
        self,
        unit: str,
        start_bound: Optional[Any],
        end_bound: Optional[Any],
        ) -> None: ...

    def get_frame_units(self) -> str: ...

    def get_lower_bound(self) -> WindowFrameBound: ...

    def get_upper_bound(self) -> WindowFrameBound: ...


class WindowFrameBound:
    def is_current_row(self) -> bool: ...

    def is_preceding(self) -> bool: ...

    def is_following(self) -> bool: ...

    def get_offset(self) -> Optional[int]: ...

    def is_unbounded(self) -> bool: ...

