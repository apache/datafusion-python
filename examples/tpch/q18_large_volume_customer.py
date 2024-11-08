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
TPC-H Problem Statement Query 18:

The Large Volume Customer Query finds a list of the top 100 customers who have ever placed large
quantity orders. The query lists the customer name, customer key, the order key, date and total
price and the quantity for the order.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.
"""

from datafusion import SessionContext, col, lit, functions as F
from util import get_data_path

QUANTITY = 300

# Load the dataframes we need

ctx = SessionContext()

df_customer = ctx.read_parquet(get_data_path("customer.parquet")).select(
    "c_custkey", "c_name"
)
df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select(
    "o_orderkey", "o_custkey", "o_orderdate", "o_totalprice"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
    "l_orderkey", "l_quantity", "l_extendedprice"
)

df = df_lineitem.aggregate(
    [col("l_orderkey")], [F.sum(col("l_quantity")).alias("total_quantity")]
)

# Limit to orders in which the total quantity is above a threshold
df = df.filter(col("total_quantity") > lit(QUANTITY))

# We've identified the orders of interest, now join the additional data
# we are required to report on
df = df.join(df_orders, left_on=["l_orderkey"], right_on=["o_orderkey"], how="inner")
df = df.join(df_customer, left_on=["o_custkey"], right_on=["c_custkey"], how="inner")

df = df.select(
    "c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice", "total_quantity"
)

df = df.sort(col("o_totalprice").sort(ascending=False), col("o_orderdate").sort())

df.show()
