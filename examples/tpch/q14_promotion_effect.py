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

from datetime import date

from datafusion import SessionContext, col, lit
from datafusion import functions as F
from util import get_data_path

MONTH_START = date(1995, 9, 1)
MONTH_END = date(1995, 10, 1)

# Load the dataframes we need

ctx = SessionContext()

df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
    "l_partkey", "l_shipdate", "l_extendedprice", "l_discount"
)
df_part = ctx.read_parquet(get_data_path("part.parquet")).select("p_partkey", "p_type")


# Restrict the line items to the month of interest, join the matching part
# rows, and aggregate revenue totals with a ``filter`` clause on the promo
# sum — the DataFrame form of SQL ``sum(... ) FILTER (WHERE ...)``.
revenue = col("l_extendedprice") * (lit(1.0) - col("l_discount"))
is_promo = F.starts_with(col("p_type"), lit("PROMO"))

df = (
    df_lineitem.filter(
        col("l_shipdate") >= lit(MONTH_START),
        col("l_shipdate") < lit(MONTH_END),
    )
    .join(df_part, left_on="l_partkey", right_on="p_partkey")
    .aggregate(
        [],
        [
            F.sum(revenue, filter=is_promo).alias("promo_revenue"),
            F.sum(revenue).alias("total_revenue"),
        ],
    )
    .select(
        (lit(100.0) * col("promo_revenue") / col("total_revenue")).alias(
            "promo_revenue"
        )
    )
)

df.show()
