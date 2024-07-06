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
"""

import pyarrow as pa
from datafusion import SessionContext, col, lit, functions as F
from util import get_data_path

BRAND = "Brand#45"
TYPE_TO_IGNORE = "MEDIUM POLISHED"
SIZES_OF_INTEREST = [49, 14, 23, 45, 19, 3, 36, 9]

# Load the dataframes we need

ctx = SessionContext()

df_part = ctx.read_parquet(get_data_path("part.parquet")).select_columns(
    "p_partkey", "p_brand", "p_type", "p_size"
)
df_partsupp = ctx.read_parquet(get_data_path("partsupp.parquet")).select_columns(
    "ps_suppkey", "ps_partkey"
)
df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select_columns(
    "s_suppkey", "s_comment"
)

df_unwanted_suppliers = df_supplier.filter(
    ~F.regexp_match(col("s_comment"), lit("Customer.?*Complaints")).is_null()
)

# Remove unwanted suppliers
df_partsupp = df_partsupp.join(
    df_unwanted_suppliers, (["ps_suppkey"], ["s_suppkey"]), "anti"
)

# Select the parts we are interested in
df_part = df_part.filter(col("p_brand") != lit(BRAND))
df_part = df_part.filter(
    F.substring(col("p_type"), lit(0), lit(len(TYPE_TO_IGNORE) + 1))
    != lit(TYPE_TO_IGNORE)
)

# Python conversion of integer to literal casts it to int64 but the data for
# part size is stored as an int32, so perform a cast. Then check to find if the part
# size is within the array of possible sizes by checking the position of it is not
# null.
p_sizes = F.make_array(*[lit(s).cast(pa.int32()) for s in SIZES_OF_INTEREST])
df_part = df_part.filter(~F.array_position(p_sizes, col("p_size")).is_null())

df = df_part.join(df_partsupp, (["p_partkey"], ["ps_partkey"]), "inner")

df = df.select_columns("p_brand", "p_type", "p_size", "ps_suppkey").distinct()

df = df.aggregate(
    [col("p_brand"), col("p_type"), col("p_size")],
    [F.count(col("ps_suppkey")).alias("supplier_cnt")],
)

df = df.sort(col("supplier_cnt").sort(ascending=False))

df.show()
