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
TPC-H Problem Statement Query 15:

The Top Supplier Query finds the supplier who contributed the most to the overall revenue for parts
shipped during a given quarter of a given year. In case of a tie, the query lists all suppliers
whose contribution was equal to the maximum, presented in supplier number order.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.

Reference SQL (from TPC-H specification, used by the benchmark suite)::

    create view revenue0 (supplier_no, total_revenue) as
        select
                l_suppkey,
                sum(l_extendedprice * (1 - l_discount))
        from
                lineitem
        where
                l_shipdate >= date '1996-01-01'
                and l_shipdate < date '1996-01-01' + interval '3' month
        group by
                l_suppkey;
    select
        s_suppkey,
        s_name,
        s_address,
        s_phone,
        total_revenue
    from
        supplier,
        revenue0
    where
        s_suppkey = supplier_no
        and total_revenue = (
                select
                        max(total_revenue)
                from
                        revenue0
        )
    order by
        s_suppkey;
    drop view revenue0;
"""

from datetime import date

from datafusion import SessionContext, col, lit
from datafusion import functions as F
from util import get_data_path

QUARTER_START = date(1996, 1, 1)
QUARTER_END = date(1996, 4, 1)

# Load the dataframes we need

ctx = SessionContext()

df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
    "l_suppkey", "l_shipdate", "l_extendedprice", "l_discount"
)
df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select(
    "s_suppkey",
    "s_name",
    "s_address",
    "s_phone",
)

# Per-supplier revenue over the quarter of interest.
revenue = col("l_extendedprice") * (lit(1) - col("l_discount"))

per_supplier_revenue = df_lineitem.filter(
    col("l_shipdate") >= lit(QUARTER_START),
    col("l_shipdate") < lit(QUARTER_END),
).aggregate(["l_suppkey"], [F.sum(revenue).alias("total_revenue")])

# Compute the grand maximum revenue separately and join on equality — the
# DataFrame stand-in for the reference SQL's
# ``total_revenue = (select max(total_revenue) from revenue0)`` subquery.
max_revenue = per_supplier_revenue.aggregate(
    [], [F.max(col("total_revenue")).alias("max_rev")]
)

top_suppliers = per_supplier_revenue.join_on(
    max_revenue, col("total_revenue") == col("max_rev")
).select("l_suppkey", "total_revenue")

df = (
    df_supplier.join(top_suppliers, left_on="s_suppkey", right_on="l_suppkey")
    .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")
    .sort_by("s_suppkey")
)

df.show()
