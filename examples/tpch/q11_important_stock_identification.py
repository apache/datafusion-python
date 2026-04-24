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
TPC-H Problem Statement Query 11:

The Important Stock Identification Query finds, from scanning the available stock of suppliers
in a given nation, all the parts that represent a significant percentage of the total value of
all available parts. The query displays the part number and the value of those parts in
descending order of value.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.

Reference SQL (from TPC-H specification, used by the benchmark suite)::

    select
        ps_partkey,
        sum(ps_supplycost * ps_availqty) as value
    from
        partsupp,
        supplier,
        nation
    where
        ps_suppkey = s_suppkey
        and s_nationkey = n_nationkey
        and n_name = 'GERMANY'
    group by
        ps_partkey having
                sum(ps_supplycost * ps_availqty) > (
                        select
                                sum(ps_supplycost * ps_availqty) * 0.0001000000
                        from
                                partsupp,
                                supplier,
                                nation
                        where
                                ps_suppkey = s_suppkey
                                and s_nationkey = n_nationkey
                                and n_name = 'GERMANY'
                )
    order by
        value desc;
"""

from datafusion import SessionContext, WindowFrame, col, lit
from datafusion import functions as F
from datafusion.expr import Window
from util import get_data_path

NATION = "GERMANY"
FRACTION = 0.0001

# Load the dataframes we need

ctx = SessionContext()

df_supplier = ctx.read_parquet(get_data_path("supplier.parquet")).select(
    "s_suppkey", "s_nationkey"
)
df_partsupp = ctx.read_parquet(get_data_path("partsupp.parquet")).select(
    "ps_supplycost", "ps_availqty", "ps_suppkey", "ps_partkey"
)
df_nation = ctx.read_parquet(get_data_path("nation.parquet")).select(
    "n_nationkey", "n_name"
)

# Restrict to the target nation, then walk to partsupp rows via the supplier
# join. Aggregate the per-part inventory value.
df = (
    df_nation.filter(col("n_name") == NATION)
    .join(df_supplier, left_on="n_nationkey", right_on="s_nationkey")
    .join(df_partsupp, left_on="s_suppkey", right_on="ps_suppkey")
    .with_column("value", col("ps_supplycost") * col("ps_availqty"))
    .aggregate(["ps_partkey"], [F.sum(col("value")).alias("value")])
)

# A window function evaluated over the entire output produces a scalar grand
# total that can be referenced row-by-row in the filter — a DataFrame-native
# stand-in for the SQL HAVING ... > (SELECT SUM(...) * FRACTION ...) pattern.
# The default frame is "UNBOUNDED PRECEDING to CURRENT ROW"; override to the
# full partition for the grand total.
whole_frame = WindowFrame("rows", None, None)

df = (
    df.with_column(
        "total_value", F.sum(col("value")).over(Window(window_frame=whole_frame))
    )
    .filter(col("value") / col("total_value") >= lit(FRACTION))
    .select("ps_partkey", "value")
    .sort(col("value").sort(ascending=False))
)

df.show()
