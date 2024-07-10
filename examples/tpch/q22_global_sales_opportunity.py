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
TPC-H Problem Statement Query 22:

This query counts how many customers within a specific range of country codes have not placed
orders for 7 years but who have a greater than average “positive” account balance. It also reflects
the magnitude of that balance. Country code is defined as the first two characters of c_phone.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.
"""

from datafusion import SessionContext, WindowFrame, col, lit, functions as F
from util import get_data_path

NATION_CODES = [13, 31, 23, 29, 30, 18, 17]

# Load the dataframes we need

ctx = SessionContext()

df_customer = ctx.read_parquet(get_data_path("customer.parquet")).select_columns(
    "c_phone", "c_acctbal", "c_custkey"
)
df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select_columns(
    "o_custkey"
)

# The nation code is a two digit number, but we need to convert it to a string literal
nation_codes = F.make_array(*[lit(str(n)) for n in NATION_CODES])

# Use the substring operation to extract the first two charaters of the phone number
df = df_customer.with_column("cntrycode", F.substring(col("c_phone"), lit(0), lit(3)))

# Limit our search to customers with some balance and in the country code above
df = df.filter(col("c_acctbal") > lit(0.0))
df = df.filter(~F.array_position(nation_codes, col("cntrycode")).is_null())

# Compute the average balance. By default, the window frame is from unbounded preceeding to the
# current row. We want our frame to cover the entire data frame.
window_frame = WindowFrame("rows", None, None)
df = df.with_column(
    "avg_balance", F.window("avg", [col("c_acctbal")], window_frame=window_frame)
)

df.show()
# Limit results to customers with above average balance
df = df.filter(col("c_acctbal") > col("avg_balance"))

# Limit results to customers with no orders
df = df.join(df_orders, (["c_custkey"], ["o_custkey"]), "anti")

# Count up the customers and the balances
df = df.aggregate(
    [col("cntrycode")],
    [
        F.count(col("c_custkey")).alias("numcust"),
        F.sum(col("c_acctbal")).alias("totacctbal"),
    ],
)

df = df.sort(col("cntrycode").sort())

df.show()
