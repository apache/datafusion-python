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

"""Col class."""

from datafusion.expr import Expr


class Col:
    """Create a column expression.

    This helper class allows an extra syntax of creating columns using the __getattr__
    method.
    """

    def __call__(self, value: str) -> Expr:
        """Create a column expression."""
        return Expr.column(value)

    def __getattr__(self, value: str) -> Expr:
        """Create a column using attribute syntax."""
        # For autocomplete to work with IPython
        if value.startswith("__wrapped__"):
            return getattr(type(self), value)

        return Expr.column(value)


col: Col = Col()
column: Col = Col()
__all__ = ["col", "column"]
