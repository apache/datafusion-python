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
    """The SQL dialect an :py:class:`Unparser` writes.

    The dialect decides how the generated SQL is spelled - most visibly how
    identifiers are quoted - so the same logical plan produces different SQL
    text for each dialect.
    """

    def __init__(self, dialect: unparser_internal.Dialect) -> None:
        """This constructor is not typically called by the end user."""
        self.dialect = dialect

    @staticmethod
    def default() -> "Dialect":
        """Create a new default dialect.

        This dialect leaves identifiers unquoted.

        Example usage:

        >>> ctx = dfn.SessionContext()
        >>> _ = ctx.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]}, name="t")
        >>> plan = ctx.sql("SELECT a FROM t").logical_plan()
        >>> Unparser(Dialect.default()).plan_to_sql(plan)
        'SELECT t.a FROM t'
        """
        return Dialect(unparser_internal.Dialect.default())

    @staticmethod
    def mysql() -> "Dialect":
        """Create a new MySQL dialect.

        This dialect quotes identifiers with backticks.

        Example usage:

        >>> ctx = dfn.SessionContext()
        >>> _ = ctx.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]}, name="t")
        >>> plan = ctx.sql("SELECT a FROM t").logical_plan()
        >>> Unparser(Dialect.mysql()).plan_to_sql(plan)
        'SELECT `t`.`a` FROM `t`'
        """
        return Dialect(unparser_internal.Dialect.mysql())

    @staticmethod
    def postgres() -> "Dialect":
        """Create a new PostgreSQL dialect.

        This dialect quotes identifiers with double quotes.

        Example usage:

        >>> ctx = dfn.SessionContext()
        >>> _ = ctx.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]}, name="t")
        >>> plan = ctx.sql("SELECT a FROM t").logical_plan()
        >>> Unparser(Dialect.postgres()).plan_to_sql(plan)
        'SELECT "t"."a" FROM "t"'
        """
        return Dialect(unparser_internal.Dialect.postgres())

    @staticmethod
    def sqlite() -> "Dialect":
        """Create a new SQLite dialect.

        This dialect quotes identifiers with backticks.

        Example usage:

        >>> ctx = dfn.SessionContext()
        >>> _ = ctx.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]}, name="t")
        >>> plan = ctx.sql("SELECT a FROM t").logical_plan()
        >>> Unparser(Dialect.sqlite()).plan_to_sql(plan)
        'SELECT `t`.`a` FROM `t`'
        """
        return Dialect(unparser_internal.Dialect.sqlite())

    @staticmethod
    def duckdb() -> "Dialect":
        """Create a new DuckDB dialect.

        This dialect quotes identifiers with double quotes.

        Example usage:

        >>> ctx = dfn.SessionContext()
        >>> _ = ctx.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]}, name="t")
        >>> plan = ctx.sql("SELECT a FROM t").logical_plan()
        >>> Unparser(Dialect.duckdb()).plan_to_sql(plan)
        'SELECT "t"."a" FROM "t"'
        """
        return Dialect(unparser_internal.Dialect.duckdb())


class Unparser:
    """Converts a :py:class:`~datafusion.plan.LogicalPlan` back into SQL text."""

    def __init__(self, dialect: Dialect) -> None:
        """This constructor is not typically called by the end user."""
        self.unparser = unparser_internal.Unparser(dialect.dialect)

    def plan_to_sql(self, plan: LogicalPlan) -> str:
        """Convert a logical plan to a SQL string.

        Example usage:

        >>> ctx = dfn.SessionContext()
        >>> _ = ctx.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]}, name="t")
        >>> plan = ctx.sql("SELECT a, b FROM t WHERE a > 1").logical_plan()
        >>> Unparser(Dialect.default()).plan_to_sql(plan)
        'SELECT t.a, t.b FROM t WHERE (t.a > 1)'
        """
        return self.unparser.plan_to_sql(plan._raw_plan)

    def with_pretty(self, pretty: bool) -> "Unparser":
        """Set the pretty flag.

        When set, redundant parentheses are omitted from the generated SQL.
        The unparser is modified in place and returned, so the call can be
        chained.

        Example usage:

        >>> ctx = dfn.SessionContext()
        >>> _ = ctx.from_pydict({"a": [1, 2, 3], "b": [10, 20, 30]}, name="t")
        >>> plan = ctx.sql("SELECT a, b FROM t WHERE a > 1").logical_plan()
        >>> Unparser(Dialect.default()).plan_to_sql(plan)
        'SELECT t.a, t.b FROM t WHERE (t.a > 1)'
        >>> Unparser(Dialect.default()).with_pretty(True).plan_to_sql(plan)
        'SELECT t.a, t.b FROM t WHERE t.a > 1'
        """
        self.unparser = self.unparser.with_pretty(pretty)
        return self


__all__ = [
    "Dialect",
    "Unparser",
]
