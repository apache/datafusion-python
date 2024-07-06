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
TPC-H Problem Statement Query 20:

The Potential Part Promotion query identifies suppliers who have an excess of a given part
available; an excess is defined to be more than 50% of the parts like the given part that the
supplier shipped in a given year for a given nation. Only parts whose names share a certain naming
convention are considered.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.
"""

from datetime import datetime
import pyarrow as pa
from datafusion import SessionContext, col, lit, functions as F
from util import get_data_path

COLOR_OF_INTEREST = "forest"
DATE_OF_INTEREST = "1994-01-01"
NATION_OF_INTEREST = "CANADA"

# Load the dataframes we need

ctx = SessionContext()

df_part = ctx.read_parquet(get_data_path("part.parquet")).select_columns(
    "p_partkey", "p_name"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select_columns(
    "l_shipdate", "l_partkey", "l_suppkey", "l_quantity"
)
df_partsupp = ctx.read_parquet(get_data_path("partsupp.parquet")).select_columns(
    "ps_partkey", "ps_suppkey", "ps_availqty"
)
df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select_columns(
    "s_suppkey", "s_address", "s_name", "s_nationkey"
)
df_nation = ctx.read_parquet(get_data_path("nation.parquet")).select_columns(
    "n_nationkey", "n_name"
)

date = datetime.strptime(DATE_OF_INTEREST, "%Y-%m-%d").date()

interval = pa.scalar((0, 365, 0), type=pa.month_day_nano_interval())

# Filter down dataframes
df_nation = df_nation.filter(col("n_name") == lit(NATION_OF_INTEREST))
df_part = df_part.filter(
    F.substring(col("p_name"), lit(0), lit(len(COLOR_OF_INTEREST) + 1))
    == lit(COLOR_OF_INTEREST)
)

df = df_lineitem.filter(col("l_shipdate") >= lit(date)).filter(
    col("l_shipdate") < lit(date) + lit(interval)
)

# This will filter down the line items to the parts of interest
df = df.join(df_part, (["l_partkey"], ["p_partkey"]), "inner")

# Compute the total sold and limit ourselves to indivdual supplier/part combinations
df = df.aggregate(
    [col("l_partkey"), col("l_suppkey")], [F.sum(col("l_quantity")).alias("total_sold")]
)

df = df.join(
    df_partsupp, (["l_partkey", "l_suppkey"], ["ps_partkey", "ps_suppkey"]), "inner"
)

# Find cases of excess quantity
df.filter(col("ps_availqty") > lit(0.5) * col("total_sold"))

# We could do these joins earlier, but now limit to the nation of interest suppliers
df = df.join(df_supplier, (["ps_suppkey"], ["s_suppkey"]), "inner")
df = df.join(df_nation, (["s_nationkey"], ["n_nationkey"]), "inner")

# Restrict to the requested data per the problem statement
df = df.select_columns("s_name", "s_address").distinct()

df = df.sort(col("s_name").sort())

df.show()
