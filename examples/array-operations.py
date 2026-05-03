# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Array operations in DataFusion Python.

Runnable reference for the idiomatic array-building and array-inspection
patterns. No external data is required -- the example constructs all inputs
through ``from_pydict``.

Topics covered:

- ``F.make_array`` to build a literal array expression.
- ``F.array_position`` and ``F.in_list`` for membership tests.
- ``F.array_length`` and ``F.array_element`` for inspecting an aggregated
  array.
- ``F.array_agg(distinct=True, filter=...)`` for building two related arrays
  per group in one pass, and filtering groups by array size afterwards.

Run with::

    python examples/array-operations.py
"""

from datafusion import SessionContext, col, lit
from datafusion import functions as F

ctx = SessionContext()


# ---------------------------------------------------------------------------
# 1. Membership tests: in_list vs. array_position / make_array
# ---------------------------------------------------------------------------

shipments = ctx.from_pydict(
    {
        "order_id": [1, 2, 3, 4, 5],
        "shipmode": ["MAIL", "SHIP", "AIR", "TRUCK", "RAIL"],
    }
)

print("\n== in_list: is shipmode one of {MAIL, SHIP}? ==")
shipments.filter(F.in_list(col("shipmode"), [lit("MAIL"), lit("SHIP")])).show()

print("\n== array_position / make_array: same question via a literal array ==")
shipments.filter(
    ~F.array_position(F.make_array(lit("MAIL"), lit("SHIP")), col("shipmode")).is_null()
).show()


# ---------------------------------------------------------------------------
# 2. array_agg with filter to inspect groups of two related arrays
# ---------------------------------------------------------------------------
#
# Input represents line items per order, each fulfilled by one supplier. The
# `failed` column marks whether the supplier met the commit date. We want to
# find orders with multiple suppliers where exactly one of them failed, and
# report that single failing supplier.

line_items = ctx.from_pydict(
    {
        "order_id": [1, 1, 1, 2, 2, 3, 3, 3, 3],
        "supplier_id": [100, 101, 102, 200, 201, 300, 301, 302, 303],
        "failed": [False, True, False, False, False, True, False, False, False],
    }
)

grouped = line_items.aggregate(
    [col("order_id")],
    [
        F.array_agg(col("supplier_id"), distinct=True).alias("all_suppliers"),
        F.array_agg(
            col("supplier_id"),
            filter=col("failed"),
            distinct=True,
        ).alias("failed_suppliers"),
    ],
)

print("\n== per-order supplier arrays ==")
grouped.sort(col("order_id").sort()).show()

print("\n== orders with >1 supplier and exactly one failure ==")
singled_out = grouped.filter(
    (F.array_length(col("failed_suppliers")) == lit(1))
    & (F.array_length(col("all_suppliers")) > lit(1))
).select(
    col("order_id"),
    F.array_element(col("failed_suppliers"), lit(1)).alias("bad_supplier"),
)
singled_out.sort(col("order_id").sort()).show()
