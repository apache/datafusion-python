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
TPC-H Problem Statement Query 7:

The Volume Shipping Query finds, for two given nations, the gross discounted revenues derived from
lineitems in which parts were shipped from a supplier in either nation to a customer in the other
nation during 1995 and 1996. The query lists the supplier nation, the customer nation, the year,
and the revenue from shipments that took place in that year. The query orders the answer by
Supplier nation, Customer nation, and year (all ascending).

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.
"""

from datetime import datetime
import pyarrow as pa
from datafusion import SessionContext, col, lit, functions as F
from util import get_data_path

# Variables of interest to query over

nation_1 = lit("FRANCE")
nation_2 = lit("GERMANY")

START_DATE = "1995-01-01"
END_DATE = "1996-12-31"

start_date = lit(datetime.strptime(START_DATE, "%Y-%m-%d").date())
end_date = lit(datetime.strptime(END_DATE, "%Y-%m-%d").date())


# Load the dataframes we need

ctx = SessionContext()

df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select(
    "s_suppkey", "s_nationkey"
)
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
    "l_shipdate", "l_extendedprice", "l_discount", "l_suppkey", "l_orderkey"
)
df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select(
    "o_orderkey", "o_custkey"
)
df_customer = ctx.read_parquet(get_data_path("customer.parquet")).select(
    "c_custkey", "c_nationkey"
)
df_nation = ctx.read_parquet(get_data_path("nation.parquet")).select(
    "n_nationkey", "n_name"
)


# Filter to time of interest
df_lineitem = df_lineitem.filter(col("l_shipdate") >= start_date).filter(
    col("l_shipdate") <= end_date
)


# A simpler way to do the following operation is to use a filter, but we also want to demonstrate
# how to use case statements. Here we are assigning `n_name` to be itself when it is either of
# the two nations of interest. Since there is no `otherwise()` statement, any values that do
# not match these will result in a null value and then get filtered out.
#
# To do the same using a simple filter would be:
# df_nation = df_nation.filter((F.col("n_name") == nation_1) | (F.col("n_name") == nation_2))
df_nation = df_nation.with_column(
    "n_name",
    F.case(col("n_name"))
    .when(nation_1, col("n_name"))
    .when(nation_2, col("n_name"))
    .end(),
).filter(~col("n_name").is_null())


# Limit suppliers to either nation
df_supplier = df_supplier.join(
    df_nation, left_on=["s_nationkey"], right_on=["n_nationkey"], how="inner"
).select(col("s_suppkey"), col("n_name").alias("supp_nation"))

# Limit customers to either nation
df_customer = df_customer.join(
    df_nation, left_on=["c_nationkey"], right_on=["n_nationkey"], how="inner"
).select(col("c_custkey"), col("n_name").alias("cust_nation"))

# Join up all the data frames from line items, and make sure the supplier and customer are in
# different nations.
df = (
    df_lineitem.join(
        df_orders, left_on=["l_orderkey"], right_on=["o_orderkey"], how="inner"
    )
    .join(df_customer, left_on=["o_custkey"], right_on=["c_custkey"], how="inner")
    .join(df_supplier, left_on=["l_suppkey"], right_on=["s_suppkey"], how="inner")
    .filter(col("cust_nation") != col("supp_nation"))
)

# Extract out two values for every line item
df = df.with_column(
    "l_year", F.datepart(lit("year"), col("l_shipdate")).cast(pa.int32())
).with_column("volume", col("l_extendedprice") * (lit(1.0) - col("l_discount")))

# Aggregate the results
df = df.aggregate(
    [col("supp_nation"), col("cust_nation"), col("l_year")],
    [F.sum(col("volume")).alias("revenue")],
)

# Sort based on problem statement requirements
df = df.sort(col("supp_nation").sort(), col("cust_nation").sort(), col("l_year").sort())

df.show()
