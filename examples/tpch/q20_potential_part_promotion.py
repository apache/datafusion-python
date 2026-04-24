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
TPC-H Problem Statement Query 20:

The Potential Part Promotion query identifies suppliers who have an excess of a given part
available; an excess is defined to be more than 50% of the parts like the given part that the
supplier shipped in a given year for a given nation. Only parts whose names share a certain naming
convention are considered.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.

Reference SQL (from TPC-H specification, used by the benchmark suite)::

    select
        s_name,
        s_address
    from
        supplier,
        nation
    where
        s_suppkey in (
                select
                        ps_suppkey
                from
                        partsupp
                where
                        ps_partkey in (
                                select
                                        p_partkey
                                from
                                        part
                                where
                                        p_name like 'forest%'
                        )
                        and ps_availqty > (
                                select
                                        0.5 * sum(l_quantity)
                                from
                                        lineitem
                                where
                                        l_partkey = ps_partkey
                                        and l_suppkey = ps_suppkey
                                        and l_shipdate >= date '1994-01-01'
                                        and l_shipdate < date '1994-01-01' + interval '1' year
                        )
        )
        and s_nationkey = n_nationkey
        and n_name = 'CANADA'
    order by
        s_name;
"""

from datetime import datetime

import pyarrow as pa
from datafusion import SessionContext, col, lit
from datafusion import functions as F
from util import get_data_path

COLOR_OF_INTEREST = "forest"
DATE_OF_INTEREST = "1994-01-01"
NATION_OF_INTEREST = "CANADA"

# Load the dataframes we need

ctx = SessionContext()

df_part = ctx.read_parquet(get_data_path("part.parquet")).select("p_partkey", "p_name")
df_lineitem = ctx.read_parquet(get_data_path("lineitem.parquet")).select(
    "l_shipdate", "l_partkey", "l_suppkey", "l_quantity"
)
df_partsupp = ctx.read_parquet(get_data_path("partsupp.parquet")).select(
    "ps_partkey", "ps_suppkey", "ps_availqty"
)
df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select(
    "s_suppkey", "s_address", "s_name", "s_nationkey"
)
df_nation = ctx.read_parquet(get_data_path("nation.parquet")).select(
    "n_nationkey", "n_name"
)

date = datetime.strptime(DATE_OF_INTEREST, "%Y-%m-%d").date()

interval = pa.scalar((0, 365, 0), type=pa.month_day_nano_interval())

# Filter down dataframes. ``starts_with`` reads more naturally than an
# explicit substring slice and maps directly to the reference SQL's
# ``p_name like 'forest%'`` clause.
df_nation = df_nation.filter(col("n_name") == NATION_OF_INTEREST)
df_part = df_part.filter(F.starts_with(col("p_name"), lit(COLOR_OF_INTEREST)))

# Compute the total quantity of interesting parts shipped by each (part,
# supplier) pair within the year of interest.
totals = (
    df_lineitem.filter(
        col("l_shipdate") >= lit(date),
        col("l_shipdate") < lit(date) + lit(interval),
    )
    .join(df_part, left_on="l_partkey", right_on="p_partkey")
    .aggregate(
        ["l_partkey", "l_suppkey"],
        [F.sum(col("l_quantity")).alias("total_sold")],
    )
)

# Keep only (part, supplier) pairs whose available quantity exceeds 50% of
# the total shipped. The result already contains one row per supplier of
# interest, so we can semi-join the supplier table rather than inner-join
# and deduplicate afterwards.
excess_suppliers = (
    df_partsupp.join(
        totals,
        left_on=["ps_partkey", "ps_suppkey"],
        right_on=["l_partkey", "l_suppkey"],
    )
    .filter(col("ps_availqty") > lit(0.5) * col("total_sold"))
    .select(col("ps_suppkey").alias("suppkey"))
    .distinct()
)

# Limit to suppliers in the nation of interest and pick out the two
# requested columns.
df = (
    df_supplier.join(df_nation, left_on="s_nationkey", right_on="n_nationkey")
    .join(excess_suppliers, left_on="s_suppkey", right_on="suppkey", how="semi")
    .select("s_name", "s_address")
    .sort_by("s_name")
)

df.show()
