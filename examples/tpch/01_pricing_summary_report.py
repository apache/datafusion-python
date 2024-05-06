import pyarrow as pa
import datafusion
from datafusion import SessionContext, col, lit, functions as F

"""
The Pricing Summary Report Query provides a summary pricing report for all lineitems shipped as of a given date.
The date is within 60 - 120 days of the greatest ship date contained in the database. The query lists totals for
extended price, discounted extended price, discounted extended price plus tax, average quantity, average extended
price, and average discount. These aggregates are grouped by RETURNFLAG and LINESTATUS, and listed in
ascending order of RETURNFLAG and LINESTATUS. A count of the number of lineitems in each group is
included.
"""

ctx = SessionContext()

df = ctx.read_parquet("data/lineitem.parquet")

# It may be that the date can be hard coded, based on examples shown.
# This approach will work with any date range in the provided data set.

greatest_ship_date = df.aggregate(
    [], [F.max(col("l_shipdate")).alias("shipdate")]
).collect()[0]["shipdate"][0]

# From the given problem, this is how close to the last date in the database we
# want to report results for. It should be between 60-120 days before the end.
days_before_final = 68

# Note: this is a hack on setting the values. It should be set differently once
# https://github.com/apache/datafusion-python/issues/665 is resolved.
interval = pa.scalar((0, 0, days_before_final), type=pa.month_day_nano_interval())

print("Final date in database:", greatest_ship_date)

# Filter data to the dates of interest
df = df.filter(col("l_shipdate") <= lit(greatest_ship_date) - lit(interval))

# Aggregate the results

df = df.aggregate(
    [col("l_returnflag"), col("l_linestatus")],
    [
        F.sum(col("l_quantity")).alias("sum_qty"),
        F.sum(col("l_extendedprice")).alias("sum_base_price"),
        F.sum(col("l_extendedprice") * (lit(1.0) - col("l_discount"))).alias(
            "sum_disc_price"
        ),
        F.sum(
            col("l_extendedprice")
            * (lit(1.0) - col("l_discount"))
            * (lit(1.0) + col("l_tax"))
        ).alias("sum_charge"),
        F.avg(col("l_quantity")).alias("avg_qty"),
        F.avg(col("l_extendedprice")).alias("avg_price"),
        F.avg(col("l_discount")).alias("avg_disc"),
        F.count(col("l_returnflag")).alias(
            "count_order"
        ),  # Counting any column should return same result
    ],
)

# Sort per the expected result

df = df.sort(col("l_returnflag").sort(), col("l_linestatus").sort())

# Note: There appears to be a discrepancy between what is returned here and what is in the generated
# answers file for the case of return flag N and line status O, but I did not investigate further.

df.show()
