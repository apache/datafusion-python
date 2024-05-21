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
"""

from datafusion import SessionContext, col, lit, functions as F
from util import get_data_path

NATION_OF_INTEREST = "SAUDI ARABIA"

# Load the dataframes we need

ctx = SessionContext()

df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select_columns(
    "o_orderkey", "o_orderstatus"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select_columns(
    "l_orderkey", "l_receiptdate", "l_commitdate", "l_suppkey"
)
df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select_columns(
    "s_suppkey", "s_name", "s_nationkey"
)
df_nation = ctx.read_parquet(get_data_path("nation.parquet")).select_columns(
    "n_nationkey", "n_name"
)

# Limit to suppliers in the nation of interest
df_suppliers_of_interest = df_nation.filter(col("n_name") == lit(NATION_OF_INTEREST))

df_suppliers_of_interest = df_suppliers_of_interest.join(
    df_supplier, (["n_nationkey"], ["s_nationkey"]), "inner"
)

# Find the failed orders and all their line items
df = df_orders.filter(col("o_orderstatus") == lit("F"))

df = df_lineitem.join(df, (["l_orderkey"], ["o_orderkey"]), "inner")

# Identify the line items for which the order is failed due to.
df = df.with_column(
    "failed_supp",
    F.case(col("l_receiptdate") > col("l_commitdate"))
    .when(lit(True), col("l_suppkey"))
    .end(),
)

# There are other ways we could do this but the purpose of this example is to work with rows where
# an element is an array of values. In this case, we will create two columns of arrays. One will be
# an array of all of the suppliers who made up this order. That way we can filter the dataframe for
# only orders where this array is larger than one for multiple supplier orders. The second column
# is all of the suppliers who failed to make their commitment. We can filter the second column for
# arrays with size one. That combination will give us orders that had multiple suppliers where only
# one failed. Use distinct=True in the blow aggregation so we don't get multipe line items from the
# same supplier reported in either array.
df = df.aggregate(
    [col("o_orderkey")],
    [
        F.array_agg(col("l_suppkey"), distinct=True).alias("all_suppliers"),
        F.array_agg(col("failed_supp"), distinct=True).alias("failed_suppliers"),
    ],
)

# Remove the null entries that will get returned by array_agg so we can test to see where we only
# have a single failed supplier in a multiple supplier order
df = df.with_column(
    "failed_suppliers", F.array_remove(col("failed_suppliers"), lit(None))
)

# This is the check described above which will identify single failed supplier in a multiple
# supplier order.
df = df.filter(F.array_length(col("failed_suppliers")) == lit(1)).filter(
    F.array_length(col("all_suppliers")) > lit(1)
)

# Since we have an array we know is exactly one element long, we can extract that single value.
df = df.select(
    col("o_orderkey"), F.array_element(col("failed_suppliers"), lit(1)).alias("suppkey")
)

# Join to the supplier of interest list for the nation of interest
df = df.join(df_suppliers_of_interest, (["suppkey"], ["s_suppkey"]), "inner")

# Count how many orders that supplier is the only failed supplier for
df = df.aggregate([col("s_name")], [F.count(col("o_orderkey")).alias("numwait")])

# Return in descending order
df = df.sort(col("numwait").sort(ascending=False), col("s_name").sort())

df = df.limit(100)

df.show()
