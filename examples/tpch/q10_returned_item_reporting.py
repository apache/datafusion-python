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
TPC-H Problem Statement Query 10:

The Returned Item Reporting Query finds the top 20 customers, in terms of their effect on lost
revenue for a given quarter, who have returned parts. The query considers only parts that were
ordered in the specified quarter. The query lists the customer's name, address, nation, phone
number, account balance, comment information and revenue lost. The customers are listed in
descending order of lost revenue. Revenue lost is defined as
sum(l_extendedprice*(1-l_discount)) for all qualifying lineitems.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.
"""

from datetime import datetime
import pyarrow as pa
from datafusion import SessionContext, col, lit, functions as F
from util import get_data_path

DATE_START_OF_QUARTER = "1993-10-01"

date_start_of_quarter = lit(datetime.strptime(DATE_START_OF_QUARTER, "%Y-%m-%d").date())

interval_one_quarter = lit(pa.scalar((0, 92, 0), type=pa.month_day_nano_interval()))

# Load the dataframes we need

ctx = SessionContext()

df_customer = ctx.read_parquet(get_data_path("customer.parquet")).select(
    "c_custkey",
    "c_nationkey",
    "c_name",
    "c_acctbal",
    "c_address",
    "c_phone",
    "c_comment",
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
    "l_extendedprice", "l_discount", "l_orderkey", "l_returnflag"
)
df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select(
    "o_orderkey", "o_custkey", "o_orderdate"
)
df_nation = ctx.read_parquet(get_data_path("nation.parquet")).select(
    "n_nationkey", "n_name", "n_regionkey"
)

# limit to returns
df_lineitem = df_lineitem.filter(col("l_returnflag") == lit("R"))


# Rather than aggregate by all of the customer fields as you might do looking at the specification,
# we can aggregate by o_custkey and then join in the customer data at the end.

df = df_orders.filter(col("o_orderdate") >= date_start_of_quarter).filter(
    col("o_orderdate") < date_start_of_quarter + interval_one_quarter
)

df = df.join(df_lineitem, left_on=["o_orderkey"], right_on=["l_orderkey"], how="inner")

# Compute the revenue
df = df.aggregate(
    [col("o_custkey")],
    [F.sum(col("l_extendedprice") * (lit(1) - col("l_discount"))).alias("revenue")],
)

# Now join in the customer data
df = df.join(df_customer, left_on=["o_custkey"], right_on=["c_custkey"], how="inner")
df = df.join(df_nation, left_on=["c_nationkey"], right_on=["n_nationkey"], how="inner")

# These are the columns the problem statement requires
df = df.select(
    "c_custkey",
    "c_name",
    "revenue",
    "c_acctbal",
    "n_name",
    "c_address",
    "c_phone",
    "c_comment",
)

# Sort the results in descending order
df = df.sort(col("revenue").sort(ascending=False))

# Only return the top 20 results
df = df.limit(20)

df.show()
