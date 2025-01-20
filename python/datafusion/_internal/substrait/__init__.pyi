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

from .. import SessionContext, LogicalPlan

class Plan:
    def encode(self) -> bytes: ...

class Serde:
    @staticmethod
    def serialize(sql: str, ctx: SessionContext, path: str): ...
    @staticmethod
    def serialize_to_plan(sql: str, ctx: SessionContext) -> Plan: ...
    @staticmethod
    def serialize_bytes(sql: str, ctx: SessionContext) -> bytes: ...
    @staticmethod
    def deserialize(path: str) -> Plan: ...
    @staticmethod
    def deserialize_bytes(proto_bytes: bytes) -> Plan: ...

class Producer:
    @staticmethod
    def to_substrait_plan(plan: LogicalPlan, ctx: SessionContext) -> Plan: ...

class Consumer:
    @staticmethod
    def from_substrait_plan(ctx: SessionContext, plan: Plan) -> LogicalPlan: ...
