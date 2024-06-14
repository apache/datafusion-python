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
TPC-H Problem Statement Query 5:

The Local Supplier Volume Query lists for each nation in a region the revenue volume that resulted
from lineitem transactions in which the customer ordering parts and the supplier filling them were
both within that nation. The query is run in order to determine whether to institute local
distribution centers in a given region. The query considers only parts ordered in a given year. The
query displays the nations and revenue volume in descending order by revenue. Revenue volume for all
qualifying lineitems in a particular nation is defined as sum(l_extendedprice * (1 - l_discount)).

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.
"""

from datetime import datetime
import pyarrow as pa
from datafusion import SessionContext, col, lit, functions as F
from util import get_data_path


DATE_OF_INTEREST = "1994-01-01"
INTERVAL_DAYS = 365
REGION_OF_INTEREST = "ASIA"

date = datetime.strptime(DATE_OF_INTEREST, "%Y-%m-%d").date()

interval = pa.scalar((0, INTERVAL_DAYS, 0), type=pa.month_day_nano_interval())

# Load the dataframes we need

ctx = SessionContext()

df_customer = ctx.read_parquet(get_data_path("customer.parquet")).select_columns(
    "c_custkey", "c_nationkey"
)
df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select_columns(
    "o_custkey", "o_orderkey", "o_orderdate"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select_columns(
    "l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"
)
df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select_columns(
    "s_suppkey", "s_nationkey"
)
df_nation = ctx.read_parquet(get_data_path("nation.parquet")).select_columns(
    "n_nationkey", "n_regionkey", "n_name"
)
df_region = ctx.read_parquet(get_data_path("region.parquet")).select_columns(
    "r_regionkey", "r_name"
)

# Restrict dataframes to cases of interest
df_orders = df_orders.filter(col("o_orderdate") >= lit(date)).filter(
    col("o_orderdate") < lit(date) + lit(interval)
)

df_region = df_region.filter(col("r_name") == lit(REGION_OF_INTEREST))

# Join all the dataframes

df = (
    df_customer.join(df_orders, (["c_custkey"], ["o_custkey"]), how="inner")
    .join(df_lineitem, (["o_orderkey"], ["l_orderkey"]), how="inner")
    .join(
        df_supplier,
        (["l_suppkey", "c_nationkey"], ["s_suppkey", "s_nationkey"]),
        how="inner",
    )
    .join(df_nation, (["s_nationkey"], ["n_nationkey"]), how="inner")
    .join(df_region, (["n_regionkey"], ["r_regionkey"]), how="inner")
)

# Compute the final result

df = df.aggregate(
    [col("n_name")],
    [F.sum(col("l_extendedprice") * (lit(1.0) - col("l_discount"))).alias("revenue")],
)

# Sort in descending order

df = df.sort(col("revenue").sort(ascending=False))

df.show()
