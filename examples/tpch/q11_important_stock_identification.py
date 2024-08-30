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
TPC-H Problem Statement Query 11:

The Important Stock Identification Query finds, from scanning the available stock of suppliers
in a given nation, all the parts that represent a significant percentage of the total value of
all available parts. The query displays the part number and the value of those parts in
descending order of value.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.
"""

from datafusion import SessionContext, WindowFrame, col, lit, functions as F
from util import get_data_path

NATION = "GERMANY"
FRACTION = 0.0001

# Load the dataframes we need

ctx = SessionContext()

df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select_columns(
    "s_suppkey", "s_nationkey"
)
df_partsupp = ctx.read_parquet(get_data_path("partsupp.parquet")).select_columns(
    "ps_supplycost", "ps_availqty", "ps_suppkey", "ps_partkey"
)
df_nation = ctx.read_parquet(get_data_path("nation.parquet")).select_columns(
    "n_nationkey", "n_name"
)

# limit to returns
df_nation = df_nation.filter(col("n_name") == lit(NATION))

# Find part supplies of within this target nation

df = df_nation.join(df_supplier, (["n_nationkey"], ["s_nationkey"]), how="inner")

df = df.join(df_partsupp, (["s_suppkey"], ["ps_suppkey"]), how="inner")


# Compute the value of individual parts
df = df.with_column("value", col("ps_supplycost") * col("ps_availqty"))

# Compute total value of specific parts
df = df.aggregate([col("ps_partkey")], [F.sum(col("value")).alias("value")])

# By default window functions go from unbounded preceding to current row, but we want
# to compute this sum across all rows
window_frame = WindowFrame("rows", None, None)

df = df.with_column(
    "total_value", F.window("sum", [col("value")], window_frame=window_frame)
)

# Limit to the parts for which there is a significant value based on the fraction of the total
df = df.filter(col("value") / col("total_value") >= lit(FRACTION))

# We only need to report on these two columns
df = df.select_columns("ps_partkey", "value")

# Sort in descending order of value
df = df.sort(col("value").sort(ascending=False))

df.show()
