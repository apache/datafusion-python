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
TPC-H Problem Statement Query 6:

The Forecasting Revenue Change Query considers all the lineitems shipped in a given year with
discounts between DISCOUNT-0.01 and DISCOUNT+0.01. The query lists the amount by which the total
revenue would have increased if these discounts had been eliminated for lineitems with l_quantity
less than quantity. Note that the potential revenue increase is equal to the sum of
[l_extendedprice * l_discount] for all lineitems with discounts and quantities in the qualifying
range.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.

Reference SQL (from TPC-H specification, used by the benchmark suite)::

    select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= date '1994-01-01'
        and l_shipdate < date '1994-01-01' + interval '1' year
        and l_discount between 0.06 - 0.01 and 0.06 + 0.01
        and l_quantity < 24;
"""

from datetime import date

from datafusion import SessionContext, col, lit
from datafusion import functions as F
from util import get_data_path

# Variables from the example query

YEAR_START = date(1994, 1, 1)
YEAR_END = date(1995, 1, 1)
DISCOUT = 0.06
DELTA = 0.01
QUANTITY = 24

# Load the dataframes we need

ctx = SessionContext()

df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
    "l_shipdate", "l_quantity", "l_extendedprice", "l_discount"
)

# Filter down to lineitems of interest

df = df_lineitem.filter(
    col("l_shipdate") >= lit(YEAR_START),
    col("l_shipdate") < lit(YEAR_END),
    col("l_discount").between(lit(DISCOUT - DELTA), lit(DISCOUT + DELTA)),
    col("l_quantity") < QUANTITY,
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

# Note: the output value from this query may be dependent on the size of the database generated
print(f"Potential lost revenue: {revenue:.2f}")
