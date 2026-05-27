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

"""
TPC-H Problem Statement Query 12:

The Shipping Modes and Order Priority Query counts, by ship mode, for lineitems actually received
by customers in a given year, the number of lineitems belonging to orders for which the
l_receiptdate exceeds the l_commitdate for two different specified ship modes. Only lineitems that
were actually shipped before the l_commitdate are considered. The late lineitems are partitioned
into two groups, those with priority URGENT or HIGH, and those with a priority other than URGENT or
HIGH.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.

Reference SQL (from TPC-H specification, used by the benchmark suite)::

    select
        l_shipmode,
        sum(case
                when o_orderpriority = '1-URGENT'
                        or o_orderpriority = '2-HIGH'
                        then 1
                else 0
        end) as high_line_count,
        sum(case
                when o_orderpriority <> '1-URGENT'
                        and o_orderpriority <> '2-HIGH'
                        then 1
                else 0
        end) as low_line_count
    from
        orders,
        lineitem
    where
        o_orderkey = l_orderkey
        and l_shipmode in ('MAIL', 'SHIP')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= date '1994-01-01'
        and l_receiptdate < date '1994-01-01' + interval '1' year
    group by
        l_shipmode
    order by
        l_shipmode;
"""

from datetime import date

from datafusion import SessionContext, col, lit
from datafusion import functions as F
from util import get_data_path

SHIP_MODE_1 = "MAIL"
SHIP_MODE_2 = "SHIP"
YEAR_START = date(1994, 1, 1)
YEAR_END = date(1995, 1, 1)

# Load the dataframes we need

ctx = SessionContext()

df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select(
    "o_orderkey", "o_orderpriority"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
    "l_orderkey", "l_shipmode", "l_commitdate", "l_shipdate", "l_receiptdate"
)


df = df_lineitem.filter(
    col("l_receiptdate") >= lit(YEAR_START),
    col("l_receiptdate") < lit(YEAR_END),
    # ``in_list`` maps directly to ``l_shipmode in (...)`` from the SQL.
    F.in_list(col("l_shipmode"), [lit(SHIP_MODE_1), lit(SHIP_MODE_2)]),
    col("l_shipdate") < col("l_commitdate"),
    col("l_commitdate") < col("l_receiptdate"),
).join(df_orders, left_on="l_orderkey", right_on="o_orderkey")

# Flag each line item as belonging to a high-priority order or not.
high_priorities = [lit("1-URGENT"), lit("2-HIGH")]
is_high = F.in_list(col("o_orderpriority"), high_priorities)
is_low = F.in_list(col("o_orderpriority"), high_priorities, negated=True)

# Count the high-priority and low-priority lineitems per ship mode via the
# ``filter`` kwarg on ``F.count`` (DataFrame form of SQL's ``count(*)
# FILTER (WHERE ...)``).
df = df.aggregate(
    ["l_shipmode"],
    [
        F.count(col("o_orderkey"), filter=is_high).alias("high_line_count"),
        F.count(col("o_orderkey"), filter=is_low).alias("low_line_count"),
    ],
).sort_by("l_shipmode")

df.show()
