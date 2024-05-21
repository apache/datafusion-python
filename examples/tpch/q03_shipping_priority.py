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
TPC-H Problem Statement Query 3:

The Shipping Priority Query retrieves the shipping priority and potential revenue, defined as the
sum of l_extendedprice * (1-l_discount), of the orders having the largest revenue among those that
had not been shipped as of a given date. Orders are listed in decreasing order of revenue. If more
than 10 unshipped orders exist, only the 10 orders with the largest revenue are listed.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.
"""

from datafusion import SessionContext, col, lit, functions as F
from util import get_data_path

SEGMENT_OF_INTEREST = "BUILDING"
DATE_OF_INTEREST = "1995-03-15"

# Load the dataframes we need

ctx = SessionContext()

df_customer = ctx.read_parquet(get_data_path("customer.parquet")).select_columns(
    "c_mktsegment", "c_custkey"
)
df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select_columns(
    "o_orderdate", "o_shippriority", "o_custkey", "o_orderkey"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select_columns(
    "l_orderkey", "l_extendedprice", "l_discount", "l_shipdate"
)

# Limit dataframes to the rows of interest

df_customer = df_customer.filter(col("c_mktsegment") == lit(SEGMENT_OF_INTEREST))
df_orders = df_orders.filter(col("o_orderdate") < lit(DATE_OF_INTEREST))
df_lineitem = df_lineitem.filter(col("l_shipdate") > lit(DATE_OF_INTEREST))

# Join all 3 dataframes

df = df_customer.join(df_orders, (["c_custkey"], ["o_custkey"]), how="inner").join(
    df_lineitem, (["o_orderkey"], ["l_orderkey"]), how="inner"
)

# Compute the revenue

df = df.aggregate(
    [col("l_orderkey")],
    [
        F.first_value(col("o_orderdate")).alias("o_orderdate"),
        F.first_value(col("o_shippriority")).alias("o_shippriority"),
        F.sum(col("l_extendedprice") * (lit(1.0) - col("l_discount"))).alias("revenue"),
    ],
)

# Sort by priority

df = df.sort(col("revenue").sort(ascending=False), col("o_orderdate").sort())

# Only return 10 results

df = df.limit(10)

# Change the order that the columns are reported in just to match the spec

df = df.select_columns("l_orderkey", "revenue", "o_orderdate", "o_shippriority")

# Show result

df.show()
