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
"""

from datetime import datetime
import pyarrow as pa
from datafusion import SessionContext, WindowFrame, col, lit, functions as F
from util import get_data_path

DATE = "1996-01-01"

date_of_interest = lit(datetime.strptime(DATE, "%Y-%m-%d").date())

interval_3_months = lit(pa.scalar((0, 91, 0), type=pa.month_day_nano_interval()))

# Load the dataframes we need

ctx = SessionContext()

df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select_columns(
    "l_suppkey", "l_shipdate", "l_extendedprice", "l_discount"
)
df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select_columns(
    "s_suppkey",
    "s_name",
    "s_address",
    "s_phone",
)

# Limit line items to the quarter of interest
df_lineitem = df_lineitem.filter(col("l_shipdate") >= date_of_interest).filter(
    col("l_shipdate") < date_of_interest + interval_3_months
)

df = df_lineitem.aggregate(
    [col("l_suppkey")],
    [
        F.sum(col("l_extendedprice") * (lit(1) - col("l_discount"))).alias(
            "total_revenue"
        )
    ],
)

# Use a window function to find the maximum revenue across the entire dataframe
window_frame = WindowFrame("rows", None, None)
df = df.with_column(
    "max_revenue", F.window("max", [col("total_revenue")], window_frame=window_frame)
)

# Find all suppliers whose total revenue is the same as the maximum
df = df.filter(col("total_revenue") == col("max_revenue"))

# Now that we know the supplier(s) with maximum revenue, get the rest of their information
# from the supplier table
df = df.join(df_supplier, (["l_suppkey"], ["s_suppkey"]), "inner")

# Return only the colums requested
df = df.select_columns("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")

# If we have more than one, sort by supplier number (suppkey)
df = df.sort(col("s_suppkey").sort())

df.show()
