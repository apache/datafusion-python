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
TPC-H Problem Statement Query 19:

The Discounted Revenue query finds the gross discounted revenue for all orders for three different
types of parts that were shipped by air and delivered in person. Parts are selected based on the
combination of specific brands, a list of containers, and a range of sizes.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.
"""

import pyarrow as pa
from datafusion import SessionContext, col, lit, udf, functions as F
from util import get_data_path

items_of_interest = {
    "Brand#12": {
        "min_quantity": 1,
        "containers": ["SM CASE", "SM BOX", "SM PACK", "SM PKG"],
        "max_size": 5,
    },
    "Brand#23": {
        "min_quantity": 10,
        "containers": ["MED BAG", "MED BOX", "MED PKG", "MED PACK"],
        "max_size": 10,
    },
    "Brand#34": {
        "min_quantity": 20,
        "containers": ["LG CASE", "LG BOX", "LG PACK", "LG PKG"],
        "max_size": 15,
    },
}

# Load the dataframes we need

ctx = SessionContext()

df_part = ctx.read_parquet(get_data_path("part.parquet")).select_columns(
    "p_partkey", "p_brand", "p_container", "p_size"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select_columns(
    "l_partkey",
    "l_quantity",
    "l_shipmode",
    "l_shipinstruct",
    "l_extendedprice",
    "l_discount",
)

# These limitations apply to all line items, so go ahead and do them first

df = df_lineitem.filter(col("l_shipinstruct") == lit("DELIVER IN PERSON"))

df = df.filter(
    (col("l_shipmode") == lit("AIR")) | (col("l_shipmode") == lit("AIR REG"))
)

df = df.join(df_part, (["l_partkey"], ["p_partkey"]), "inner")


# Create the user defined function (UDF) definition that does the work
def is_of_interest(
    brand_arr: pa.Array,
    container_arr: pa.Array,
    quantity_arr: pa.Array,
    size_arr: pa.Array,
) -> pa.Array:
    """
    The purpose of this function is to demonstrate how a UDF works, taking as input a pyarrow Array
    and generating a resultant Array. The length of the inputs should match and there should be the
    same number of rows in the output.
    """
    result = []
    for idx, brand in enumerate(brand_arr):
        brand = brand.as_py()
        if brand in items_of_interest:
            values_of_interest = items_of_interest[brand]

            container_matches = (
                container_arr[idx].as_py() in values_of_interest["containers"]
            )

            quantity = quantity_arr[idx].as_py()
            quantity_matches = (
                values_of_interest["min_quantity"]
                <= quantity
                <= values_of_interest["min_quantity"] + 10
            )

            size = size_arr[idx].as_py()
            size_matches = 1 <= size <= values_of_interest["max_size"]

            result.append(container_matches and quantity_matches and size_matches)
        else:
            result.append(False)

    return pa.array(result)


# Turn the above function into a UDF that DataFusion can understand
is_of_interest_udf = udf(
    is_of_interest,
    [pa.utf8(), pa.utf8(), pa.decimal128(15, 2), pa.int32()],
    pa.bool_(),
    "stable",
)

# Filter results using the above UDF
df = df.filter(
    is_of_interest_udf(
        col("p_brand"), col("p_container"), col("l_quantity"), col("p_size")
    )
)

df = df.aggregate(
    [],
    [F.sum(col("l_extendedprice") * (lit(1) - col("l_discount"))).alias("revenue")],
)

df.show()
