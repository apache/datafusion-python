from .. import SessionContext, LogicalPlan


class Plan:
    def encode(self) -> bytes:
        ...
    
class Serde:
    @staticmethod
    def serialize(sql: str, ctx: SessionContext, path: str):
        ...
    
    @staticmethod
    def serialize_to_plan(sql: str, ctx: SessionContext) -> Plan:
        ...

    @staticmethod
    def serialize_bytes(sql: str, ctx: SessionContext) -> bytes:
        ...

    @staticmethod
    def deserialize(path: str) -> Plan:
        ...

    @staticmethod
    def deserialize_bytes(proto_bytes: bytes) -> Plan:
        ...

class Producer:
    @staticmethod
    def to_substrait_plan(plan: LogicalPlan, ctx: SessionContext) -> Plan:
        ...

class Consumer:
    @staticmethod
    def from_substrait_plan(ctx: SessionContext, plan: Plan) -> LogicalPlan:
        ...
