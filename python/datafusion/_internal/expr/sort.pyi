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

from typing import List, Optional

from .. import LogicalPlan
from ..common import DFSchema
from .sort_expr import SortExpr

class Sort:
    def sort_exprs(self) -> List[SortExpr]: ...
    def get_fetch_val(self) -> Optional[int]: ...
    def input(self) -> List[LogicalPlan]: ...
    def schema(self) -> DFSchema: ...
