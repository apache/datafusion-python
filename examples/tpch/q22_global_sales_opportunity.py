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
TPC-H Problem Statement Query 22:

This query counts how many customers within a specific range of country codes have not placed
orders for 7 years but who have a greater than average “positive” account balance. It also reflects
the magnitude of that balance. Country code is defined as the first two characters of c_phone.

The above problem statement text is copyrighted by the Transaction Processing Performance Council
as part of their TPC Benchmark H Specification revision 2.18.0.

Reference SQL (from TPC-H specification, used by the benchmark suite)::

    select
        cntrycode,
        count(*) as numcust,
        sum(c_acctbal) as totacctbal
    from
        (
                select
                        substring(c_phone from 1 for 2) as cntrycode,
                        c_acctbal
                from
                        customer
                where
                        substring(c_phone from 1 for 2) in
                                ('13', '31', '23', '29', '30', '18', '17')
                        and c_acctbal > (
                                select
                                        avg(c_acctbal)
                                from
                                        customer
                                where
                                        c_acctbal > 0.00
                                        and substring(c_phone from 1 for 2) in
                                                ('13', '31', '23', '29', '30', '18', '17')
                        )
                        and not exists (
                                select
                                        *
                                from
                                        orders
                                where
                                        o_custkey = c_custkey
                        )
        ) as custsale
    group by
        cntrycode
    order by
        cntrycode;
"""

from datafusion import SessionContext, WindowFrame, col, lit
from datafusion import functions as F
from datafusion.expr import Window
from util import get_data_path

NATION_CODES = [13, 31, 23, 29, 30, 18, 17]

# Load the dataframes we need

ctx = SessionContext()

df_customer = ctx.read_parquet(get_data_path("customer.parquet")).select(
    "c_phone", "c_acctbal", "c_custkey"
)
df_orders = ctx.read_parquet(get_data_path("orders.parquet")).select("o_custkey")

# Country code is the two-digit prefix of the phone number.
nation_codes = [lit(str(n)) for n in NATION_CODES]

# Start from customers with a positive balance in one of the target country
# codes, then attach the grand-mean balance via a whole-frame window so we
# can filter per row — DataFrame stand-in for the SQL's scalar ``(select
# avg(c_acctbal) ... )`` subquery.
whole_frame = WindowFrame("rows", None, None)

df = (
    df_customer.with_column("cntrycode", F.left(col("c_phone"), lit(2)))
    .filter(
        col("c_acctbal") > 0.0,
        F.in_list(col("cntrycode"), nation_codes),
    )
    .with_column(
        "avg_balance",
        F.avg(col("c_acctbal")).over(Window(window_frame=whole_frame)),
    )
    .filter(col("c_acctbal") > col("avg_balance"))
    # Keep only customers with no orders (anti join = NOT EXISTS).
    .join(df_orders, left_on="c_custkey", right_on="o_custkey", how="anti")
    .aggregate(
        ["cntrycode"],
        [
            F.count_star().alias("numcust"),
            F.sum(col("c_acctbal")).alias("totacctbal"),
        ],
    )
    .sort_by("cntrycode")
)

df.show()
