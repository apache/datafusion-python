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

Reference SQL (from TPC-H specification, used by the benchmark suite)::

    select
        sum(l_extendedprice* (1 - l_discount)) as revenue
    from
        lineitem,
        part
    where
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#21'
                and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                and l_quantity >= 8 and l_quantity <= 8 + 10
                and p_size between 1 and 5
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#13'
                and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                and l_quantity >= 20 and l_quantity <= 20 + 10
                and p_size between 1 and 10
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        )
        or
        (
                p_partkey = l_partkey
                and p_brand = 'Brand#52'
                and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                and l_quantity >= 30 and l_quantity <= 30 + 10
                and p_size between 1 and 15
                and l_shipmode in ('AIR', 'AIR REG')
                and l_shipinstruct = 'DELIVER IN PERSON'
        );
"""

from datafusion import SessionContext, col, lit
from datafusion import functions as F
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

df_part = ctx.read_parquet(get_data_path("part.parquet")).select(
    "p_partkey", "p_brand", "p_container", "p_size"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
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

df = df.join(df_part, left_on=["l_partkey"], right_on=["p_partkey"], how="inner")


# Build one OR-combined predicate per brand. Each disjunct encodes the
# brand-specific container list, quantity window, and size range from the
# reference SQL. This mirrors the SQL ``where (... brand A ...) or (... brand
# B ...) or (... brand C ...)`` form directly, without a UDF.
def _brand_predicate(
    brand: str, min_quantity: int, containers: list[str], max_size: int
):
    return (
        (col("p_brand") == lit(brand))
        & F.in_list(col("p_container"), [lit(c) for c in containers])
        & (col("l_quantity") >= lit(min_quantity))
        & (col("l_quantity") <= lit(min_quantity + 10))
        & (col("p_size") >= lit(1))
        & (col("p_size") <= lit(max_size))
    )


predicate = None
for brand, params in items_of_interest.items():
    part_predicate = _brand_predicate(
        brand,
        params["min_quantity"],
        params["containers"],
        params["max_size"],
    )
    predicate = part_predicate if predicate is None else predicate | part_predicate

df = df.filter(predicate)

df = df.aggregate(
    [],
    [F.sum(col("l_extendedprice") * (lit(1) - col("l_discount"))).alias("revenue")],
)

df.show()
