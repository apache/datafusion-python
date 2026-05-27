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
TPC-H Problem Statement Query 21:

The Suppliers Who Kept Orders Waiting query identifies suppliers, for a given nation, whose product
was part of a multi-supplier order (with current status of 'F') where they were the only supplier
who failed to meet the committed delivery date.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.

Reference SQL (from TPC-H specification, used by the benchmark suite)::

    select
        s_name,
        count(*) as numwait
    from
        supplier,
        lineitem l1,
        orders,
        nation
    where
        s_suppkey = l1.l_suppkey
        and o_orderkey = l1.l_orderkey
        and o_orderstatus = 'F'
        and l1.l_receiptdate > l1.l_commitdate
        and exists (
                select
                        *
                from
                        lineitem l2
                where
                        l2.l_orderkey = l1.l_orderkey
                        and l2.l_suppkey <> l1.l_suppkey
        )
        and not exists (
                select
                        *
                from
                        lineitem l3
                where
                        l3.l_orderkey = l1.l_orderkey
                        and l3.l_suppkey <> l1.l_suppkey
                        and l3.l_receiptdate > l3.l_commitdate
        )
        and s_nationkey = n_nationkey
        and n_name = 'SAUDI ARABIA'
    group by
        s_name
    order by
        numwait desc,
        s_name limit 100;
"""

from datafusion import SessionContext, col
from datafusion import functions as F
from util import get_data_path

NATION_OF_INTEREST = "SAUDI ARABIA"

# Load the dataframes we need

ctx = SessionContext()

df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select(
    "o_orderkey", "o_orderstatus"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
    "l_orderkey", "l_receiptdate", "l_commitdate", "l_suppkey"
)
df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select(
    "s_suppkey", "s_name", "s_nationkey"
)
df_nation = ctx.read_parquet(get_data_path("nation.parquet")).select(
    "n_nationkey", "n_name"
)

# Limit to suppliers in the nation of interest
df_suppliers_of_interest = df_nation.filter(col("n_name") == NATION_OF_INTEREST).join(
    df_supplier, left_on="n_nationkey", right_on="s_nationkey"
)

# Line items for orders that have status 'F'. This is the candidate set of
# (order, supplier) pairs we reason about below.
failed_order_lineitems = df_lineitem.join(
    df_orders.filter(col("o_orderstatus") == "F"),
    left_on="l_orderkey",
    right_on="o_orderkey",
)

# Line items whose receipt was late. This corresponds to ``l1`` in the
# reference SQL.
late_lineitems = failed_order_lineitems.filter(
    col("l_receiptdate") > col("l_commitdate")
)

# Orders that had more than one distinct supplier. Expressed as
# ``count(distinct l_suppkey) > 1``. Stands in for the reference SQL's
# ``exists (... l2.l_suppkey <> l1.l_suppkey ...)`` subquery.
multi_supplier_orders = (
    failed_order_lineitems.select("l_orderkey", "l_suppkey")
    .distinct()
    .aggregate(["l_orderkey"], [F.count_star().alias("n_suppliers")])
    .filter(col("n_suppliers") > 1)
    .select("l_orderkey")
)

# Orders where exactly one distinct supplier was late. Stands in for the
# reference SQL's ``not exists (... l3.l_suppkey <> l1.l_suppkey and l3 is
# also late ...)`` subquery: if only one supplier on the order was late,
# nobody else on the same order was late.
single_late_supplier_orders = (
    late_lineitems.select("l_orderkey", "l_suppkey")
    .distinct()
    .aggregate(["l_orderkey"], [F.count_star().alias("n_late_suppliers")])
    .filter(col("n_late_suppliers") == 1)
    .select("l_orderkey")
)

# Keep late line items whose order qualifies on both counts, attach the
# supplier name for suppliers in the nation of interest, count one row per
# qualifying order, and return the top 100.
df = (
    late_lineitems.join(multi_supplier_orders, on="l_orderkey", how="semi")
    .join(single_late_supplier_orders, on="l_orderkey", how="semi")
    .join(df_suppliers_of_interest, left_on="l_suppkey", right_on="s_suppkey")
    .aggregate(["s_name"], [F.count_star().alias("numwait")])
    .sort(col("numwait").sort(ascending=False), "s_name")
    .limit(100)
)

df.show()
