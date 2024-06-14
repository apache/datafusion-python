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
"""

from datetime import datetime
import pyarrow as pa
from datafusion import SessionContext, col, lit, functions as F
from util import get_data_path

# Ideally we could put 3 months into the interval. See note below.
INTERVAL_DAYS = 92
DATE_OF_INTEREST = "1993-07-01"

# Load the dataframes we need

ctx = SessionContext()

df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select_columns(
    "o_orderdate", "o_orderpriority", "o_orderkey"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select_columns(
    "l_orderkey", "l_commitdate", "l_receiptdate"
)

# Create a date object from the string
date = datetime.strptime(DATE_OF_INTEREST, "%Y-%m-%d").date()

interval = pa.scalar((0, INTERVAL_DAYS, 0), type=pa.month_day_nano_interval())

# Limit results to cases where commitment date before receipt date
# Aggregate the results so we only get one row to join with the order table.
# Alterately, and likely more idomatic is instead of `.aggregate` you could
# do `.select_columns("l_orderkey").distinct()`. The goal here is to show
# mulitple examples of how to use Data Fusion.
df_lineitem = df_lineitem.filter(col("l_commitdate") < col("l_receiptdate")).aggregate(
    [col("l_orderkey")], []
)

# Limit orders to date range of interest
df_orders = df_orders.filter(col("o_orderdate") >= lit(date)).filter(
    col("o_orderdate") < lit(date) + lit(interval)
)

# Perform the join to find only orders for which there are lineitems outside of expected range
df = df_orders.join(df_lineitem, (["o_orderkey"], ["l_orderkey"]), how="inner")

# Based on priority, find the number of entries
df = df.aggregate(
    [col("o_orderpriority")], [F.count(col("o_orderpriority")).alias("order_count")]
)

# Sort the results
df = df.sort(col("o_orderpriority").sort())

df.show()
