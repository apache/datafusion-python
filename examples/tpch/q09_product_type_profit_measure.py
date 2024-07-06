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
TPC-H Problem Statement Query 9:

The Product Type Profit Measure Query finds, for each nation and each year, the profit for all parts
ordered in that year that contain a specified substring in their names and that were filled by a
supplier in that nation. The profit is defined as the sum of
[(l_extendedprice*(1-l_discount)) - (ps_supplycost * l_quantity)] for all lineitems describing
parts in the specified line. The query lists the nations in ascending alphabetical order and, for
each nation, the year and profit in descending order by year (most recent first).

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.
"""

import pyarrow as pa
from datafusion import SessionContext, col, lit, functions as F
from util import get_data_path

part_color = lit("green")

# Load the dataframes we need

ctx = SessionContext()

df_part = ctx.read_parquet(get_data_path("part.parquet")).select_columns(
    "p_partkey", "p_name"
)
df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select_columns(
    "s_suppkey", "s_nationkey"
)
df_partsupp = ctx.read_parquet(get_data_path("partsupp.parquet")).select_columns(
    "ps_suppkey", "ps_partkey", "ps_supplycost"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select_columns(
    "l_partkey",
    "l_extendedprice",
    "l_discount",
    "l_suppkey",
    "l_orderkey",
    "l_quantity",
)
df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select_columns(
    "o_orderkey", "o_custkey", "o_orderdate"
)
df_nation = ctx.read_parquet(get_data_path("nation.parquet")).select_columns(
    "n_nationkey", "n_name", "n_regionkey"
)

# Limit possible parts to the color specified
df = df_part.filter(F.strpos(col("p_name"), part_color) > lit(0))

# We have a series of joins that get us to limit down to the line items we need
df = df.join(df_lineitem, (["p_partkey"], ["l_partkey"]), how="inner")
df = df.join(df_supplier, (["l_suppkey"], ["s_suppkey"]), how="inner")
df = df.join(df_orders, (["l_orderkey"], ["o_orderkey"]), how="inner")
df = df.join(
    df_partsupp, (["l_suppkey", "l_partkey"], ["ps_suppkey", "ps_partkey"]), how="inner"
)
df = df.join(df_nation, (["s_nationkey"], ["n_nationkey"]), how="inner")

# Compute the intermediate values and limit down to the expressions we need
df = df.select(
    col("n_name").alias("nation"),
    F.datepart(lit("year"), col("o_orderdate")).cast(pa.int32()).alias("o_year"),
    (
        (col("l_extendedprice") * (lit(1) - col("l_discount")))
        - (col("ps_supplycost") * col("l_quantity"))
    ).alias("amount"),
)

# Sum up the values by nation and year
df = df.aggregate(
    [col("nation"), col("o_year")], [F.sum(col("amount")).alias("profit")]
)

# Sort according to the problem specification
df = df.sort(col("nation").sort(), col("o_year").sort(ascending=False))

df.show()
