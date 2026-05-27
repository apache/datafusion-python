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
TPC-H Problem Statement Query 8:

The market share for a given nation within a given region is defined as the fraction of the
revenue, the sum of [l_extendedprice * (1-l_discount)], from the products of a specified type in
that region that was supplied by suppliers from the given nation. The query determines this for the
years 1995 and 1996 presented in this order.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.

Reference SQL (from TPC-H specification, used by the benchmark suite)::

    select
        o_year,
        sum(case
                when nation = 'BRAZIL' then volume
                else 0
        end) / sum(volume) as mkt_share
    from
        (
                select
                        extract(year from o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) as volume,
                        n2.n_name as nation
                from
                        part,
                        supplier,
                        lineitem,
                        orders,
                        customer,
                        nation n1,
                        nation n2,
                        region
                where
                        p_partkey = l_partkey
                        and s_suppkey = l_suppkey
                        and l_orderkey = o_orderkey
                        and o_custkey = c_custkey
                        and c_nationkey = n1.n_nationkey
                        and n1.n_regionkey = r_regionkey
                        and r_name = 'AMERICA'
                        and s_nationkey = n2.n_nationkey
                        and o_orderdate between date '1995-01-01' and date '1996-12-31'
                        and p_type = 'ECONOMY ANODIZED STEEL'
        ) as all_nations
    group by
        o_year
    order by
        o_year;
"""

from datetime import date

import pyarrow as pa
from datafusion import SessionContext, col, lit
from datafusion import functions as F
from util import get_data_path

supplier_nation = "BRAZIL"
customer_region = "AMERICA"
part_of_interest = "ECONOMY ANODIZED STEEL"

START_DATE = date(1995, 1, 1)
END_DATE = date(1996, 12, 31)


# Load the dataframes we need

ctx = SessionContext()

df_part = ctx.read_parquet(get_data_path("part.parquet")).select("p_partkey", "p_type")
df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select(
    "s_suppkey", "s_nationkey"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
    "l_partkey", "l_extendedprice", "l_discount", "l_suppkey", "l_orderkey"
)
df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select(
    "o_orderkey", "o_custkey", "o_orderdate"
)
df_customer = ctx.read_parquet(get_data_path("customer.parquet")).select(
    "c_custkey", "c_nationkey"
)
df_nation = ctx.read_parquet(get_data_path("nation.parquet")).select(
    "n_nationkey", "n_name", "n_regionkey"
)
df_region = ctx.read_parquet(get_data_path("region.parquet")).select(
    "r_regionkey", "r_name"
)

# Limit possible parts to the one specified
df_part = df_part.filter(col("p_type") == part_of_interest)

# Limit orders to those in the specified range

df_orders = df_orders.filter(
    col("o_orderdate") >= lit(START_DATE), col("o_orderdate") <= lit(END_DATE)
)

# Pair each supplier with its nation name so every regional-customer row
# below carries the supplier's nation and can be filtered inside the
# aggregate with ``F.sum(..., filter=...)``.

df_supplier_with_nation = df_supplier.join(
    df_nation, left_on="s_nationkey", right_on="n_nationkey"
).select("s_suppkey", col("n_name").alias("supp_nation"))

# Build every (part, lineitem, order, customer) row for customers in the
# target region ordering the target part. Each row carries the supplier's
# nation so we can aggregate on it below.

df = (
    df_region.filter(col("r_name") == customer_region)
    .join(df_nation, left_on="r_regionkey", right_on="n_regionkey")
    .join(df_customer, left_on="n_nationkey", right_on="c_nationkey")
    .join(df_orders, left_on="c_custkey", right_on="o_custkey")
    .join(df_lineitem, left_on="o_orderkey", right_on="l_orderkey")
    .join(df_part, left_on="l_partkey", right_on="p_partkey")
    .join(df_supplier_with_nation, left_on="l_suppkey", right_on="s_suppkey")
    .with_columns(
        volume=col("l_extendedprice") * (lit(1.0) - col("l_discount")),
        o_year=F.datepart(lit("year"), col("o_orderdate")).cast(pa.int32()),
    )
)

# Aggregate the total and national volumes per year via the ``filter``
# kwarg on ``F.sum`` (DataFrame form of SQL ``sum(... ) FILTER (WHERE ...)``).
# ``coalesce`` handles the case where no sale came from the target nation
# for a given year.
df = (
    df.aggregate(
        ["o_year"],
        [
            F.sum(col("volume"), filter=col("supp_nation") == supplier_nation).alias(
                "national_volume"
            ),
            F.sum(col("volume")).alias("total_volume"),
        ],
    )
    .select(
        "o_year",
        (F.coalesce(col("national_volume"), lit(0.0)) / col("total_volume")).alias(
            "mkt_share"
        ),
    )
    .sort_by("o_year")
)

df.show()
