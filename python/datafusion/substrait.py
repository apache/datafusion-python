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

"""This module provides support for using substrait with datafusion.

For additional information about substrait, see https://substrait.io/ for more
information about substrait.
"""

from __future__ import annotations

from ._internal import substrait as substrait_internal

from typing import TYPE_CHECKING
from typing_extensions import deprecated
import pathlib

if TYPE_CHECKING:
    from datafusion.context import SessionContext
    from datafusion._internal import LogicalPlan

__all__ = [
    "Plan",
    "Consumer",
    "Producer",
    "Serde",
]


class Plan:
    """A class representing an encodable substrait plan."""

    def __init__(self, plan: substrait_internal.Plan) -> None:
        """Create a substrait plan.

        The user should not have to call this constructor directly. Rather, it
        should be created via :py:class:`Serde` or py:class:`Producer` classes
        in this module.
        """
        self.plan_internal = plan

    def encode(self) -> bytes:
        """Encode the plan to bytes.

        Returns:
            Encoded plan.
        """
        return self.plan_internal.encode()


@deprecated("Use `Plan` instead.")
class plan(Plan):
    """See `Plan`."""

    pass


class Serde:
    """Provides the ``Substrait`` serialization and deserialization."""

    @staticmethod
    def serialize(sql: str, ctx: SessionContext, path: str | pathlib.Path) -> None:
        """Serialize a SQL query to a Substrait plan and write it to a file.

        Args:
            sql:SQL query to serialize.
            ctx: SessionContext to use.
            path: Path to write the Substrait plan to.
        """
        return substrait_internal.Serde.serialize(sql, ctx.ctx, str(path))

    @staticmethod
    def serialize_to_plan(sql: str, ctx: SessionContext) -> Plan:
        """Serialize a SQL query to a Substrait plan.

        Args:
        sql: SQL query to serialize.
        ctx: SessionContext to use.

        Returns:
            Substrait plan.
        """
        return Plan(substrait_internal.Serde.serialize_to_plan(sql, ctx.ctx))

    @staticmethod
    def serialize_bytes(sql: str, ctx: SessionContext) -> bytes:
        """Serialize a SQL query to a Substrait plan as bytes.

        Args:
            sql: SQL query to serialize.
            ctx: SessionContext to use.

        Returns:
            Substrait plan as bytes.
        """
        return substrait_internal.Serde.serialize_bytes(sql, ctx.ctx)

    @staticmethod
    def deserialize(path: str | pathlib.Path) -> Plan:
        """Deserialize a Substrait plan from a file.

        Args:
            path: Path to read the Substrait plan from.

        Returns:
            Substrait plan.
        """
        return Plan(substrait_internal.Serde.deserialize(str(path)))

    @staticmethod
    def deserialize_bytes(proto_bytes: bytes) -> Plan:
        """Deserialize a Substrait plan from bytes.

        Args:
            proto_bytes: Bytes to read the Substrait plan from.

        Returns:
            Substrait plan.
        """
        return Plan(substrait_internal.Serde.deserialize_bytes(proto_bytes))


@deprecated("Use `Serde` instead.")
class serde(Serde):
    """See `Serde` instead."""

    pass


class Producer:
    """Generates substrait plans from a logical plan."""

    @staticmethod
    def to_substrait_plan(logical_plan: LogicalPlan, ctx: SessionContext) -> Plan:
        """Convert a DataFusion LogicalPlan to a Substrait plan.

        Args:
            logical_plan: LogicalPlan to convert.
            ctx: SessionContext to use.

        Returns:
            Substrait plan.
        """
        return Plan(
            substrait_internal.Producer.to_substrait_plan(logical_plan, ctx.ctx)
        )


@deprecated("Use `Producer` instead.")
class producer(Producer):
    """Use `Producer` instead."""

    pass


class Consumer:
    """Generates a logical plan from a substrait plan."""

    @staticmethod
    def from_substrait_plan(ctx: SessionContext, plan: Plan) -> LogicalPlan:
        """Convert a Substrait plan to a DataFusion LogicalPlan.

        Args:
            ctx: SessionContext to use.
            plan: Substrait plan to convert.

        Returns:
            LogicalPlan.
        """
        return substrait_internal.Consumer.from_substrait_plan(
            ctx.ctx, plan.plan_internal
        )


@deprecated("Use `Consumer` instead.")
class consumer(Consumer):
    """Use `Consumer` instead."""

    pass
