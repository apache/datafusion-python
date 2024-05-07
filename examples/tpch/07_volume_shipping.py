import pyarrow as pa
import datafusion
from datafusion import SessionContext, col, lit, functions as F
from datetime import datetime

"""
The Volume Shipping Query finds, for two given nations, the gross discounted revenues derived from lineitems in
which parts were shipped from a supplier in either nation to a customer in the other nation during 1995 and 1996.
The query lists the supplier nation, the customer nation, the year, and the revenue from shipments that took place in
that year. The query orders the answer by Supplier nation, Customer nation, and year (all ascending).
"""

# Variables of interest to query over

nation_1 = lit("GERMANY")
nation_2 = lit("IRAQ")

start_date = "1995-01-01"
end_date = "1996-12-31"

start_date = lit(datetime.strptime(start_date, "%Y-%m-%d").date())
end_date = lit(datetime.strptime(end_date, "%Y-%m-%d").date())


# Load the dataframes we need

ctx = SessionContext()

df_supplier = ctx.read_parquet("data/supplier.parquet").select_columns(
    "s_suppkey", "s_nationkey"
)
df_lineitem = ctx.read_parquet("data/lineitem.parquet").select_columns(
    "l_shipdate", "l_extendedprice", "l_discount", "l_suppkey", "l_orderkey"
)
df_orders = ctx.read_parquet("data/orders.parquet").select_columns(
    "o_orderkey", "o_custkey"
)
df_customer = ctx.read_parquet("data/customer.parquet").select_columns(
    "c_custkey", "c_nationkey"
)
df_nation = ctx.read_parquet("data/nation.parquet").select_columns(
    "n_nationkey", "n_name"
)


# Filter to time of interest
df_lineitem = df_lineitem.filter(col("l_shipdate") >= start_date).filter(
    col("l_shipdate") <= end_date
)


# Until https://github.com/apache/datafusion-python/issues/667 is resolved, work around
# using boolean operations to find the nation dataframe by doing a case operation. It
# would be nice to do something along the lines of:
# `.filter(col("n_name") == nation_1 or col("n_name") == nation_2)`
df_nation = df_nation.with_column(
    "of_interest",
    F.case(col("n_name"))
    .when(nation_1, lit(True))
    .when(nation_2, lit(True))
    .otherwise(lit(False)),
)

df_nation = df_nation.filter(col("of_interest"))


# Limit suppliers to either nation
df_supplier = df_supplier.join(
    df_nation, (["s_nationkey"], ["n_nationkey"]), how="inner"
).select(col("s_suppkey"), col("n_name").alias("supp_nation"))

# Limit customers to either nation
df_customer = df_customer.join(
    df_nation, (["c_nationkey"], ["n_nationkey"]), how="inner"
).select(col("c_custkey"), col("n_name").alias("cust_nation"))

# Join up all the data frames from line items, and make sure the supplier and customer are in different nations.
df = (
    df_lineitem.join(df_orders, (["l_orderkey"], ["o_orderkey"]), how="inner")
    .join(df_customer, (["o_custkey"], ["c_custkey"]), how="inner")
    .join(df_supplier, (["l_suppkey"], ["s_suppkey"]), how="inner")
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
