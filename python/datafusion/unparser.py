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

"""This module provides support for unparsing datafusion plans to SQL.

For additional information about unparsing, see https://docs.rs/datafusion-sql/latest/datafusion_sql/unparser/index.html
"""

from ._internal import unparser as unparser_internal
from .plan import LogicalPlan


class Dialect:
    """DataFusion data catalog."""

    def __init__(self, dialect: unparser_internal.Dialect) -> None:
        """This constructor is not typically called by the end user."""
        self.dialect = dialect

    @staticmethod
    def default() -> "Dialect":
        """Create a new default dialect."""
        return Dialect(unparser_internal.Dialect.default())

    @staticmethod
    def mysql() -> "Dialect":
        """Create a new MySQL dialect."""
        return Dialect(unparser_internal.Dialect.mysql())

    @staticmethod
    def postgres() -> "Dialect":
        """Create a new PostgreSQL dialect."""
        return Dialect(unparser_internal.Dialect.postgres())

    @staticmethod
    def sqlite() -> "Dialect":
        """Create a new SQLite dialect."""
        return Dialect(unparser_internal.Dialect.sqlite())

    @staticmethod
    def duckdb() -> "Dialect":
        """Create a new DuckDB dialect."""
        return Dialect(unparser_internal.Dialect.duckdb())


class Unparser:
    """DataFusion unparser."""

    def __init__(self, dialect: Dialect) -> None:
        """This constructor is not typically called by the end user."""
        self.unparser = unparser_internal.Unparser(dialect.dialect)

    def plan_to_sql(self, plan: LogicalPlan) -> str:
        """Convert a logical plan to a SQL string."""
        return self.unparser.plan_to_sql(plan._raw_plan)

    def with_pretty(self, pretty: bool) -> "Unparser":
        """Set the pretty flag."""
        self.unparser = self.unparser.with_pretty(pretty)
        return self


__all__ = [
    "Dialect",
    "Unparser",
]
