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
TPC-H Problem Statement Query 13:

This query determines the distribution of customers by the number of orders they have made,
including customers who have no record of orders, past or present. It counts and reports how many
customers have no orders, how many have 1, 2, 3, etc. A check is made to ensure that the orders
counted do not fall into one of several special categories of orders. Special categories are
identified in the order comment column by looking for a particular pattern.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.
"""

from datafusion import SessionContext, col, lit, functions as F
from util import get_data_path

WORD_1 = "special"
WORD_2 = "requests"

# Load the dataframes we need

ctx = SessionContext()

df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select_columns(
    "o_custkey", "o_comment"
)
df_customer = ctx.read_parquet(get_data_path("customer.parquet")).select_columns(
    "c_custkey"
)

# Use a regex to remove special cases
df_orders = df_orders.filter(
    F.regexp_match(col("o_comment"), lit(f"{WORD_1}.?*{WORD_2}")).is_null()
)

# Since we may have customers with no orders we must do a left join
df = df_customer.join(df_orders, (["c_custkey"], ["o_custkey"]), how="left")

# Find the number of orders for each customer
df = df.aggregate([col("c_custkey")], [F.count(col("o_custkey")).alias("c_count")])

# Ultimately we want to know the number of customers that have that customer count
df = df.aggregate([col("c_count")], [F.count(col("c_count")).alias("custdist")])

# We want to order the results by the highest number of customers per count
df = df.sort(
    col("custdist").sort(ascending=False), col("c_count").sort(ascending=False)
)

df.show()
