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

For additional information about substrait, see https://substrait.io/ for more information
about substrait.
"""

from __future__ import annotations

from ._internal import substrait as substrait_internal

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion.context import SessionContext
    from datafusion._internal import LogicalPlan


class plan:
    """A class representing an encodable substrait plan."""

    def __init__(self, plan: substrait_internal.plan) -> None:
        """Create a substrait plan.

        The user should not have to call this constructor directly. Rather, it should be created
        via ``serde`` or ``producer`` classes in this module.
        """
        self.plan_internal = plan

    def encode(self) -> bytes:
        """Encode the plan to bytes.

        Returns:
        -------
        bytes
            Encoded plan.
        """
        return self.plan_internal.encode()


class serde:
    """Provides the serialization and deserialization required to convert to and from a Substrait plan."""

    @staticmethod
    def serialize(sql: str, ctx: SessionContext, path: str) -> None:
        """Serialize a SQL query to a Substrait plan and write it to a file.

        Parameters
        ----------
        sql : str
            SQL query to serialize.
        ctx : SessionContext
            SessionContext to use.
        path : str
            Path to write the Substrait plan to.
        """
        return substrait_internal.serde.serialize(sql, ctx.ctx, path)

    @staticmethod
    def serialize_to_plan(sql: str, ctx: SessionContext) -> plan:
        """Serialize a SQL query to a Substrait plan.

        Parameters
        ----------
        sql : str
            SQL query to serialize.
        ctx : SessionContext
            SessionContext to use.

        Returns:
        -------
        plan
            Substrait plan.
        """
        return plan(substrait_internal.serde.serialize_to_plan(sql, ctx.ctx))

    @staticmethod
    def serialize_bytes(sql: str, ctx: SessionContext) -> bytes:
        """Serialize a SQL query to a Substrait plan as bytes.

        Parameters
        ----------
        sql : str
            SQL query to serialize.
        ctx : SessionContext
            SessionContext to use.

        Returns:
        -------
        bytes
            Substrait plan as bytes.
        """
        return substrait_internal.serde.serialize_bytes(sql, ctx.ctx)

    @staticmethod
    def deserialize(path: str) -> plan:
        """Deserialize a Substrait plan from a file.

        Parameters
        ----------
        path : str
            Path to read the Substrait plan from.

        Returns:
        -------
        plan
            Substrait plan.
        """
        return plan(substrait_internal.serde.deserialize(path))

    @staticmethod
    def deserialize_bytes(proto_bytes: bytes) -> plan:
        """Deserialize a Substrait plan from bytes.

        Parameters
        ----------
        proto_bytes : bytes
            Bytes to read the Substrait plan from.

        Returns:
        -------
        plan
            Substrait plan.
        """
        return plan(substrait_internal.serde.deserialize_bytes(proto_bytes))


class producer:
    """Generates substrait plans from a logical plan."""

    @staticmethod
    def to_substrait_plan(logical_plan: LogicalPlan, ctx: SessionContext) -> plan:
        """Convert a DataFusion LogicalPlan to a Substrait plan.

        Parameters
        ----------
        plan : LogicalPlan
            LogicalPlan to convert.
        ctx : SessionContext
            SessionContext to use.

        Returns:
        -------
        plan
            Substrait plan.
        """
        return plan(
            substrait_internal.producer.to_substrait_plan(logical_plan, ctx.ctx)
        )


class consumer:
    """Generates a logical plan from a substrait plan."""

    @staticmethod
    def from_substrait_plan(ctx: SessionContext, plan: plan) -> LogicalPlan:
        """Convert a Substrait plan to a DataFusion LogicalPlan.

        Parameters
        ----------
        ctx : SessionContext
            SessionContext to use.
        plan : plan
            Substrait plan to convert.

        Returns:
        -------
        LogicalPlan
            LogicalPlan.
        """
        return substrait_internal.consumer.from_substrait_plan(
            ctx.ctx, plan.plan_internal
        )
