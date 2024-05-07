import pyarrow as pa
import datafusion
from datafusion import SessionContext, col, lit, functions as F
from datetime import datetime

"""
The Local Supplier Volume Query lists for each nation in a region the revenue volume that resulted from lineitem
transactions in which the customer ordering parts and the supplier filling them were both within that nation. The
query is run in order to determine whether to institute local distribution centers in a given region. The query consid-
ers only parts ordered in a given year. The query displays the nations and revenue volume in descending order by
revenue. Revenue volume for all qualifying lineitems in a particular nation is defined as sum(l_extendedprice * (1 -
l_discount)).
"""

date_of_interest = "1994-01-01"
interval_days = 365
region_of_interest = "AFRICA"

date = datetime.strptime(date_of_interest, "%Y-%m-%d").date()

# Note: this is a hack on setting the values. It should be set differently once
# https://github.com/apache/datafusion-python/issues/665 is resolved.
interval = pa.scalar((0, 0, interval_days), type=pa.month_day_nano_interval())

# Load the dataframes we need

ctx = SessionContext()

df_customer = ctx.read_parquet("data/customer.parquet").select_columns(
    "c_custkey", "c_nationkey"
)
df_orders = ctx.read_parquet("data/orders.parquet").select_columns(
    "o_custkey", "o_orderkey", "o_orderdate"
)
df_lineitem = ctx.read_parquet("data/lineitem.parquet").select_columns(
    "l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"
)
df_supplier = ctx.read_parquet("data/supplier.parquet").select_columns(
    "s_suppkey", "s_nationkey"
)
df_nation = ctx.read_parquet("data/nation.parquet").select_columns(
    "n_nationkey", "n_regionkey", "n_name"
)
df_region = ctx.read_parquet("data/region.parquet").select_columns(
    "r_regionkey", "r_name"
)

# Restrict dataframes to cases of interest
df_orders = df_orders.filter(col("o_orderdate") >= lit(date)).filter(
    col("o_orderdate") < lit(date) + lit(interval)
)

df_region = df_region.filter(col("r_name") == lit(region_of_interest))

# Join all the dataframes

df = (
    df_customer.join(df_orders, (["c_custkey"], ["o_custkey"]), how="inner")
    .join(df_lineitem, (["o_orderkey"], ["l_orderkey"]), how="inner")
    .join(
        df_supplier,
        (["l_suppkey", "c_nationkey"], ["s_suppkey", "s_nationkey"]),
        how="inner",
    )
    .join(df_nation, (["s_nationkey"], ["n_nationkey"]), how="inner")
    .join(df_region, (["n_regionkey"], ["r_regionkey"]), how="inner")
)

# Compute the final result

df = df.aggregate(
    [col("n_name")],
    [F.sum(col("l_extendedprice") * (lit(1.0) - col("l_discount"))).alias("revenue")],
)

# Sort in descending order

df = df.sort(col("revenue").sort(ascending=False))

df.show()
