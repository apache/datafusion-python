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
TPC-H Problem Statement Query 17:

The Small-Quantity-Order Revenue Query considers parts of a given brand and with a given container
type and determines the average lineitem quantity of such parts ordered for all orders (past and
pending) in the 7-year database. What would be the average yearly gross (undiscounted) loss in
revenue if orders for these parts with a quantity of less than 20% of this average were no longer
taken?

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.
"""

from datafusion import SessionContext, WindowFrame, col, lit, functions as F
from util import get_data_path

BRAND = "Brand#23"
CONTAINER = "MED BOX"

# Load the dataframes we need

ctx = SessionContext()

df_part = ctx.read_parquet(get_data_path("part.parquet")).select_columns(
    "p_partkey", "p_brand", "p_container"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select_columns(
    "l_partkey", "l_quantity", "l_extendedprice"
)

# Limit to the problem statement's brand and container types
df = df_part.filter(col("p_brand") == lit(BRAND)).filter(
    col("p_container") == lit(CONTAINER)
)

# Combine data
df = df.join(df_lineitem, (["p_partkey"], ["l_partkey"]), "inner")

# Find the average quantity
window_frame = WindowFrame("rows", None, None)
df = df.with_column(
    "avg_quantity",
    F.window(
        "avg",
        [col("l_quantity")],
        window_frame=window_frame,
        partition_by=[col("l_partkey")],
    ),
)

df = df.filter(col("l_quantity") < lit(0.2) * col("avg_quantity"))

# Compute the total
df = df.aggregate([], [F.sum(col("l_extendedprice")).alias("total")])

# Divide by number of years in the problem statement to get average
df = df.select((col("total") / lit(7)).alias("avg_yearly"))

df.show()
