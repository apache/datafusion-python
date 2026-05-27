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

Reference SQL (from TPC-H specification, used by the benchmark suite)::

    select
        n_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue
    from
        customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
    where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and l_suppkey = s_suppkey
        and c_nationkey = s_nationkey
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'ASIA'
        and o_orderdate >= date '1994-01-01'
        and o_orderdate < date '1994-01-01' + interval '1' year
    group by
        n_name
    order by
        revenue desc;
"""

from datetime import date

from datafusion import SessionContext, col, lit
from datafusion import functions as F
from util import get_data_path

YEAR_START = date(1994, 1, 1)
YEAR_END = date(1995, 1, 1)
REGION_OF_INTEREST = "ASIA"

# Load the dataframes we need

ctx = SessionContext()

df_customer = ctx.read_parquet(get_data_path("customer.parquet")).select(
    "c_custkey", "c_nationkey"
)
df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select(
    "o_custkey", "o_orderkey", "o_orderdate"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
    "l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"
)
df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select(
    "s_suppkey", "s_nationkey"
)
df_nation = ctx.read_parquet(get_data_path("nation.parquet")).select(
    "n_nationkey", "n_regionkey", "n_name"
)
df_region = ctx.read_parquet(get_data_path("region.parquet")).select(
    "r_regionkey", "r_name"
)

# Restrict dataframes to cases of interest
df_orders = df_orders.filter(
    col("o_orderdate") >= lit(YEAR_START),
    col("o_orderdate") < lit(YEAR_END),
)

df_region = df_region.filter(col("r_name") == REGION_OF_INTEREST)

# Join all the dataframes

df = (
    df_customer.join(df_orders, left_on="c_custkey", right_on="o_custkey")
    .join(df_lineitem, left_on="o_orderkey", right_on="l_orderkey")
    .join(
        df_supplier,
        left_on=["l_suppkey", "c_nationkey"],
        right_on=["s_suppkey", "s_nationkey"],
    )
    .join(df_nation, left_on="s_nationkey", right_on="n_nationkey")
    .join(df_region, left_on="n_regionkey", right_on="r_regionkey")
)

# Compute the final result, then sort in descending order.

df = df.aggregate(
    ["n_name"],
    [F.sum(col("l_extendedprice") * (lit(1.0) - col("l_discount"))).alias("revenue")],
).sort(col("revenue").sort(ascending=False))

df.show()
