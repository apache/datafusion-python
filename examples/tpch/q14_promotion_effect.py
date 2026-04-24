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

Reference SQL (from TPC-H specification, used by the benchmark suite)::

    select
        100.00 * sum(case
                when p_type like 'PROMO%'
                        then l_extendedprice * (1 - l_discount)
                else 0
        end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
    from
        lineitem,
        part
    where
        l_partkey = p_partkey
        and l_shipdate >= date '1995-09-01'
        and l_shipdate < date '1995-09-01' + interval '1' month;
"""

from datetime import datetime

import pyarrow as pa
from datafusion import SessionContext, col, lit
from datafusion import functions as F
from util import get_data_path

DATE = "1995-09-01"

date_of_interest = lit(datetime.strptime(DATE, "%Y-%m-%d").date())

interval_one_month = lit(pa.scalar((0, 30, 0), type=pa.month_day_nano_interval()))

# Load the dataframes we need

ctx = SessionContext()

df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
    "l_partkey", "l_shipdate", "l_extendedprice", "l_discount"
)
df_part = ctx.read_parquet(get_data_path("part.parquet")).select("p_partkey", "p_type")


# Restrict the line items to the month of interest, join the matching part
# rows, and compute revenue per line item.
df = (
    df_lineitem.filter(
        col("l_shipdate") >= date_of_interest,
        col("l_shipdate") < date_of_interest + interval_one_month,
    )
    .join(df_part, left_on="l_partkey", right_on="p_partkey")
    .with_column("revenue", col("l_extendedprice") * (lit(1.0) - col("l_discount")))
)

# Sum promotional and total revenue, then compute the percentage. The
# ``F.when(...)`` form mirrors the ``case when p_type like 'PROMO%' ... else 0``
# in the reference SQL.
df = df.aggregate(
    [],
    [
        F.sum(
            F.when(
                F.starts_with(col("p_type"), lit("PROMO")), col("revenue")
            ).otherwise(lit(0.0))
        ).alias("promo_revenue"),
        F.sum(col("revenue")).alias("total_revenue"),
    ],
).select(
    (lit(100.0) * col("promo_revenue") / col("total_revenue")).alias("promo_revenue")
)

df.show()
