"""
DataFusion Python FFI query planner example.

Prints plans, showing the logical plan handed to the planner, the physical
plan returned, the effect of SET ffi_query_planner.max_rows, and two
planners nesting.
"""

import sys

try:
    from datafusion_ffi_query_planner_example import (
        MyPlannerConfig,
        MyQueryPlanner,
    )
except ImportError:
    sys.exit("build the extension first:\n  uv run maturin develop\nSee README.md.")

from datafusion import SessionConfig, SessionContext  # noqa: I001, E402


print("1. logical plan")
config = SessionConfig().with_extension(MyPlannerConfig(max_rows=5))
ctx = SessionContext(config)

ctx.sql(
    "CREATE TABLE t AS SELECT * FROM (VALUES (1), (2), (3), (4), (5), (6), (7)) AS t(a)"
)
df = ctx.sql("SELECT * FROM t")
print(df.logical_plan().display_indent())

print("\n2. physical plan returned")
planner = MyQueryPlanner()
ctx.set_query_planner(planner)

plan = df.execution_plan()
print(plan.display_indent())

print("\n3. effect of SET ffi_query_planner.max_rows")
ctx.sql("SET ffi_query_planner.max_rows = 2").collect()
plan2 = df.execution_plan()
print(plan2.display_indent())

print("\n4. two planners nesting")
outer_planner = MyQueryPlanner(fallback=planner)
ctx.set_query_planner(outer_planner)
plan3 = df.execution_plan()
print(plan3.display_indent())
