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

from typing import List, Optional, Tuple
from .. import LogicalPlan
from .base import Expr
from ..common import DFSchema


class JoinType:
    def is_outer(self) -> bool: ...

class JoinConstraint:
    ...


class Join:
    def left(self) -> LogicalPlan: ...

    def right(self) -> LogicalPlan: ...

    def on(self) -> List[Tuple[Expr, Expr]]: ...

    def filter(self) -> Optional[Expr]: ...

    def join_type(self) -> JoinType: ...

    def join_constraint(self) -> JoinConstraint: ...

    def schema(self) -> DFSchema: ...

    def null_equals_null(self) -> bool: ...

    def __name__(self) -> str: ...

