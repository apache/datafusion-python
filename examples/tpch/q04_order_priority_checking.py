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
TPC-H Problem Statement Query 4:

The Order Priority Checking Query counts the number of orders ordered in a given quarter of a given
year in which at least one lineitem was received by the customer later than its committed date. The
query lists the count of such orders for each order priority sorted in ascending priority order.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.

Reference SQL (from TPC-H specification, used by the benchmark suite)::

    select
        o_orderpriority,
        count(*) as order_count
    from
        orders
    where
        o_orderdate >= date '1993-07-01'
        and o_orderdate < date '1993-07-01' + interval '3' month
        and exists (
                select
                        *
                from
                        lineitem
                where
                        l_orderkey = o_orderkey
                        and l_commitdate < l_receiptdate
        )
    group by
        o_orderpriority
    order by
        o_orderpriority;
"""

from datetime import datetime

import pyarrow as pa
from datafusion import SessionContext, col, lit
from datafusion import functions as F
from util import get_data_path

# Ideally we could put 3 months into the interval. See note below.
INTERVAL_DAYS = 92
DATE_OF_INTEREST = "1993-07-01"

# Load the dataframes we need

ctx = SessionContext()

df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select(
    "o_orderdate", "o_orderpriority", "o_orderkey"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
    "l_orderkey", "l_commitdate", "l_receiptdate"
)

# Create a date object from the string
date = datetime.strptime(DATE_OF_INTEREST, "%Y-%m-%d").date()

interval = pa.scalar((0, INTERVAL_DAYS, 0), type=pa.month_day_nano_interval())

# Keep only orders in the quarter of interest, then restrict to those that
# have at least one late lineitem via a semi join (the DataFrame form of
# ``EXISTS`` from the reference SQL).
df_orders = df_orders.filter(
    col("o_orderdate") >= lit(date),
    col("o_orderdate") < lit(date) + lit(interval),
)

late_lineitems = df_lineitem.filter(col("l_commitdate") < col("l_receiptdate"))

df = df_orders.join(
    late_lineitems, left_on="o_orderkey", right_on="l_orderkey", how="semi"
)

# Count the number of orders in each priority group and sort.
df = df.aggregate(["o_orderpriority"], [F.count_star().alias("order_count")]).sort_by(
    "o_orderpriority"
)

df.show()
