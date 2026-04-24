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
TPC-H Problem Statement Query 16:

The Parts/Supplier Relationship Query counts the number of suppliers who can supply parts that
satisfy a particular customer's requirements. The customer is interested in parts of eight
different sizes as long as they are not of a given type, not of a given brand, and not from a
supplier who has had complaints registered at the Better Business Bureau. Results must be presented
in descending count and ascending brand, type, and size.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.

Reference SQL (from TPC-H specification, used by the benchmark suite)::

    select
        p_brand,
        p_type,
        p_size,
        count(distinct ps_suppkey) as supplier_cnt
    from
        partsupp,
        part
    where
        p_partkey = ps_partkey
        and p_brand <> 'Brand#45'
        and p_type not like 'MEDIUM POLISHED%'
        and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
        and ps_suppkey not in (
                select
                        s_suppkey
                from
                        supplier
                where
                        s_comment like '%Customer%Complaints%'
        )
    group by
        p_brand,
        p_type,
        p_size
    order by
        supplier_cnt desc,
        p_brand,
        p_type,
        p_size;
"""

from datafusion import SessionContext, col, lit
from datafusion import functions as F
from util import get_data_path

BRAND = "Brand#45"
TYPE_TO_IGNORE = "MEDIUM POLISHED"
SIZES_OF_INTEREST = [49, 14, 23, 45, 19, 3, 36, 9]

# Load the dataframes we need

ctx = SessionContext()

df_part = ctx.read_parquet(get_data_path("part.parquet")).select(
    "p_partkey", "p_brand", "p_type", "p_size"
)
df_partsupp = ctx.read_parquet(get_data_path("partsupp.parquet")).select(
    "ps_suppkey", "ps_partkey"
)
df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select(
    "s_suppkey", "s_comment"
)

df_unwanted_suppliers = df_supplier.filter(
    F.regexp_match(col("s_comment"), lit("Customer.?*Complaints")).is_not_null()
)

# Remove unwanted suppliers via an anti join (DataFrame form of NOT IN).
df_partsupp = df_partsupp.join(
    df_unwanted_suppliers, left_on="ps_suppkey", right_on="s_suppkey", how="anti"
)

# Select the parts we are interested in.
df_part = df_part.filter(
    col("p_brand") != BRAND,
    ~F.starts_with(col("p_type"), lit(TYPE_TO_IGNORE)),
    F.in_list(col("p_size"), [lit(s) for s in SIZES_OF_INTEREST]),
)

# For each (brand, type, size), count the distinct suppliers remaining.
df = (
    df_part.join(df_partsupp, left_on="p_partkey", right_on="ps_partkey")
    .select("p_brand", "p_type", "p_size", "ps_suppkey")
    .distinct()
    .aggregate(
        ["p_brand", "p_type", "p_size"],
        [F.count(col("ps_suppkey")).alias("supplier_cnt")],
    )
    .sort(
        col("supplier_cnt").sort(ascending=False),
        "p_brand",
        "p_type",
        "p_size",
    )
)

df.show()
