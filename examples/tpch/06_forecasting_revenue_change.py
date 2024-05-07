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

import pyarrow as pa
from datafusion import SessionContext, col, lit, functions as F
from datetime import datetime

"""
The Forecasting Revenue Change Query considers all the lineitems shipped in a given year with discounts between
DISCOUNT-0.01 and DISCOUNT+0.01. The query lists the amount by which the total revenue would have
increased if these discounts had been eliminated for lineitems with l_quantity less than quantity. Note that the
potential revenue increase is equal to the sum of [l_extendedprice * l_discount] for all lineitems with discounts and
quantities in the qualifying range.
"""

# Variables from the example query

date_of_interest = "1994-01-01"
discount = 0.06
delta = 0.01
quantity = 24

interval_days = 365

date = datetime.strptime(date_of_interest, "%Y-%m-%d").date()

# Note: this is a hack on setting the values. It should be set differently once
# https://github.com/apache/datafusion-python/issues/665 is resolved.
interval = pa.scalar((0, 0, interval_days), type=pa.month_day_nano_interval())

# Load the dataframes we need

ctx = SessionContext()

df_lineitem = ctx.read_parquet("data/lineitem.parquet").select_columns(
    "l_shipdate", "l_quantity", "l_extendedprice", "l_discount"
)

# Filter down to lineitems of interest

df = (
    df_lineitem.filter(col("l_shipdate") >= lit(date))
    .filter(col("l_shipdate") < lit(date) + lit(interval))
    .filter(col("l_discount") >= lit(discount) - lit(delta))
    .filter(col("l_discount") <= lit(discount) + lit(delta))
    .filter(col("l_quantity") < lit(quantity))
)

# Add up all the "lost" revenue

df = df.aggregate(
    [], [F.sum(col("l_extendedprice") * col("l_discount")).alias("revenue")]
)

# Show the single result. We could do a `show()` but since we want to demonstrate features of how
# to use Data Fusion, instead collect the result as a python object and print it out.

# collect() should give a list of record batches. This is a small query, so we should get a
# single batch back, hence the index [0]. Within each record batch we only care about the
# single column result `revenue`. Since we have only one row returned because we aggregated
# over the entire dataframe, we can index it at 0. Then convert the DoubleScalar into a
# simple python object.

revenue = df.collect()[0]["revenue"][0].as_py()

print(f"Potential lost revenue: {revenue:.2f}")
