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
TPC-H Problem Statement Query 8:

The market share for a given nation within a given region is defined as the fraction of the
revenue, the sum of [l_extendedprice * (1-l_discount)], from the products of a specified type in
that region that was supplied by suppliers from the given nation. The query determines this for the
years 1995 and 1996 presented in this order.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.
"""

from datetime import datetime
import pyarrow as pa
from datafusion import SessionContext, col, lit, functions as F
from util import get_data_path

supplier_nation = lit("BRAZIL")
customer_region = lit("AMERICA")
part_of_interest = lit("ECONOMY ANODIZED STEEL")

START_DATE = "1995-01-01"
END_DATE = "1996-12-31"

start_date = lit(datetime.strptime(START_DATE, "%Y-%m-%d").date())
end_date = lit(datetime.strptime(END_DATE, "%Y-%m-%d").date())


# Load the dataframes we need

ctx = SessionContext()

df_part = ctx.read_parquet(get_data_path("part.parquet")).select_columns(
    "p_partkey", "p_type"
)
df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select_columns(
    "s_suppkey", "s_nationkey"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select_columns(
    "l_partkey", "l_extendedprice", "l_discount", "l_suppkey", "l_orderkey"
)
df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select_columns(
    "o_orderkey", "o_custkey", "o_orderdate"
)
df_customer = ctx.read_parquet(get_data_path("customer.parquet")).select_columns(
    "c_custkey", "c_nationkey"
)
df_nation = ctx.read_parquet(get_data_path("nation.parquet")).select_columns(
    "n_nationkey", "n_name", "n_regionkey"
)
df_region = ctx.read_parquet(get_data_path("region.parquet")).select_columns(
    "r_regionkey", "r_name"
)

# Limit possible parts to the one specified
df_part = df_part.filter(col("p_type") == part_of_interest)

# Limit orders to those in the specified range

df_orders = df_orders.filter(col("o_orderdate") >= start_date).filter(
    col("o_orderdate") <= end_date
)

# Part 1: Find customers in the region

# We want customers in region specified by region_of_interest. This will be used to compute
# the total sales of the part of interest. We want to know of those sales what fraction
# was supplied by the nation of interest. There is no guarantee that the nation of
# interest is within the region of interest.

# First we find all the sales that make up the basis.

df_regional_customers = df_region.filter(col("r_name") == customer_region)

# After this join we have all of the possible sales nations
df_regional_customers = df_regional_customers.join(
    df_nation, (["r_regionkey"], ["n_regionkey"]), how="inner"
)

# Now find the possible customers
df_regional_customers = df_regional_customers.join(
    df_customer, (["n_nationkey"], ["c_nationkey"]), how="inner"
)

# Next find orders for these customers
df_regional_customers = df_regional_customers.join(
    df_orders, (["c_custkey"], ["o_custkey"]), how="inner"
)

# Find all line items from these orders
df_regional_customers = df_regional_customers.join(
    df_lineitem, (["o_orderkey"], ["l_orderkey"]), how="inner"
)

# Limit to the part of interest
df_regional_customers = df_regional_customers.join(
    df_part, (["l_partkey"], ["p_partkey"]), how="inner"
)

# Compute the volume for each line item
df_regional_customers = df_regional_customers.with_column(
    "volume", col("l_extendedprice") * (lit(1.0) - col("l_discount"))
)

# Part 2: Find suppliers from the nation

# Now that we have all of the sales of that part in the specified region, we need
# to determine which of those came from suppliers in the nation we are interested in.

df_national_suppliers = df_nation.filter(col("n_name") == supplier_nation)

# Determine the suppliers by the limited nation key we have in our single row df above
df_national_suppliers = df_national_suppliers.join(
    df_supplier, (["n_nationkey"], ["s_nationkey"]), how="inner"
)

# When we join to the customer dataframe, we don't want to confuse other columns, so only
# select the supplier key that we need
df_national_suppliers = df_national_suppliers.select_columns("s_suppkey")


# Part 3: Combine suppliers and customers and compute the market share

# Now we can do a left outer join on the suppkey. Those line items from other suppliers
# will get a null value. We can check for the existence of this null to compute a volume
# column only from suppliers in the nation we are evaluating.

df = df_regional_customers.join(
    df_national_suppliers, (["l_suppkey"], ["s_suppkey"]), how="left"
)

# Use a case statement to compute the volume sold by suppliers in the nation of interest
df = df.with_column(
    "national_volume",
    F.case(col("s_suppkey").is_null())
    .when(lit(False), col("volume"))
    .otherwise(lit(0.0)),
)

df = df.with_column(
    "o_year", F.datepart(lit("year"), col("o_orderdate")).cast(pa.int32())
)


# Lastly, sum up the results

df = df.aggregate(
    [col("o_year")],
    [
        F.sum(col("volume")).alias("volume"),
        F.sum(col("national_volume")).alias("national_volume"),
    ],
)

df = df.select(
    col("o_year"), (F.col("national_volume") / F.col("volume")).alias("mkt_share")
)

df = df.sort(col("o_year").sort())

df.show()
