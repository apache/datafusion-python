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
        and l_shipmode in ('FOB', 'SHIP')
        and l_commitdate < l_receiptdate
        and l_shipdate < l_commitdate
        and l_receiptdate >= date '1995-01-01'
        and l_receiptdate < date '1995-01-01' + interval '1' year
    group by
        l_shipmode
    order by
        l_shipmode;
"""

from datetime import datetime

import pyarrow as pa
from datafusion import SessionContext, col, lit
from datafusion import functions as F
from util import get_data_path

SHIP_MODE_1 = "MAIL"
SHIP_MODE_2 = "SHIP"
DATE_OF_INTEREST = "1994-01-01"

# Load the dataframes we need

ctx = SessionContext()

df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select(
    "o_orderkey", "o_orderpriority"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
    "l_orderkey", "l_shipmode", "l_commitdate", "l_shipdate", "l_receiptdate"
)

date = datetime.strptime(DATE_OF_INTEREST, "%Y-%m-%d").date()

interval = pa.scalar((0, 365, 0), type=pa.month_day_nano_interval())


df = df_lineitem.filter(col("l_receiptdate") >= lit(date)).filter(
    col("l_receiptdate") < lit(date) + lit(interval)
)

# Restrict to the two ship modes of interest. ``in_list`` maps directly to
# the ``l_shipmode in ('FOB', 'SHIP')`` clause of the reference SQL.
df = df.filter(F.in_list(col("l_shipmode"), [lit(SHIP_MODE_1), lit(SHIP_MODE_2)]))


# We need order priority, so join order df to line item
df = df.join(df_orders, left_on=["l_orderkey"], right_on=["o_orderkey"], how="inner")

# Restrict to line items we care about based on the problem statement.
df = df.filter(col("l_commitdate") < col("l_receiptdate"))

df = df.filter(col("l_shipdate") < col("l_commitdate"))

df = df.with_column(
    "high_line_value",
    F.case(col("o_orderpriority"))
    .when(lit("1-URGENT"), lit(1))
    .when(lit("2-HIGH"), lit(1))
    .otherwise(lit(0)),
)

# Aggregate the results
df = df.aggregate(
    [col("l_shipmode")],
    [
        F.sum(col("high_line_value")).alias("high_line_count"),
        F.count(col("high_line_value")).alias("all_lines_count"),
    ],
)

# Compute the final output
df = df.select(
    col("l_shipmode"),
    col("high_line_count"),
    (col("all_lines_count") - col("high_line_count")).alias("low_line_count"),
)

df = df.sort(col("l_shipmode").sort())

df.show()
