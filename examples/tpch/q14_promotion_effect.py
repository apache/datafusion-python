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
TPC-H Problem Statement Query 14:

The Promotion Effect Query determines what percentage of the revenue in a given year and month was
derived from promotional parts. The query considers only parts actually shipped in that month and
gives the percentage. Revenue is defined as (l_extendedprice * (1-l_discount)).

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.
"""

from datetime import datetime
import pyarrow as pa
from datafusion import SessionContext, col, lit, functions as F
from util import get_data_path

DATE = "1995-09-01"

date_of_interest = lit(datetime.strptime(DATE, "%Y-%m-%d").date())

interval_one_month = lit(pa.scalar((0, 30, 0), type=pa.month_day_nano_interval()))

# Load the dataframes we need

ctx = SessionContext()

df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select_columns(
    "l_partkey", "l_shipdate", "l_extendedprice", "l_discount"
)
df_part = ctx.read_parquet(get_data_path("part.parquet")).select_columns(
    "p_partkey", "p_type"
)


# Check part type begins with PROMO
df_part = df_part.filter(
    F.substring(col("p_type"), lit(0), lit(6)) == lit("PROMO")
).with_column("promo_factor", lit(1.0))

df_lineitem = df_lineitem.filter(col("l_shipdate") >= date_of_interest).filter(
    col("l_shipdate") < date_of_interest + interval_one_month
)

# Left join so we can sum up the promo parts different from other parts
df = df_lineitem.join(df_part, (["l_partkey"], ["p_partkey"]), "left")

# Make a factor of 1.0 if it is a promotion, 0.0 otherwise
df = df.with_column("promo_factor", F.coalesce(col("promo_factor"), lit(0.0)))
df = df.with_column("revenue", col("l_extendedprice") * (lit(1.0) - col("l_discount")))


# Sum up the promo and total revenue
df = df.aggregate(
    [],
    [
        F.sum(col("promo_factor") * col("revenue")).alias("promo_revenue"),
        F.sum(col("revenue")).alias("total_revenue"),
    ],
)

# Return the percentage of revenue from promotions
df = df.select(
    (lit(100.0) * col("promo_revenue") / col("total_revenue")).alias("promo_revenue")
)

df.show()
