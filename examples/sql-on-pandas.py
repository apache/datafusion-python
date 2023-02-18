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

import pandas as pd
from datafusion import SessionContext
from datafusion.expr import Projection, TableScan, Expr
import re

class SqlOnPandasContext:
    def __init__(self):
        self.datafusion_ctx = SessionContext()
        self.parquet_tables = {}


    def register_parquet(self, name, path):
        self.parquet_tables[name] = path
        self.datafusion_ctx.register_parquet(name, path)


    def to_pandas_expr(self, expr):
        # TODO: need python wrappers for each type of expression
        if isinstance(expr, Expr):
            str = "{}".format(expr)
            x = re.findall("Expr\([_a-z]+\.([_a-z]+)\)", str)
            return x[0]


    def to_pandas_df(self, plan):
        # recurse down first to translate inputs into pandas data frames
        inputs = [self.to_pandas_df(x) for x in plan.inputs()]

        # get Python wrapper for logical operator node
        node = plan.to_logical_node()

        if isinstance(node, Projection):
            args = [self.to_pandas_expr(expr) for expr in node.projections()]
            return inputs[0][args]
        elif isinstance(node, TableScan):
            return pd.read_parquet(self.parquet_tables[node.table_name()])
        else:
            raise Exception("unsupported logical operator: {}".format(type(node)))

    def sql(self, sql):
        datafusion_df = self.datafusion_ctx.sql(sql)
        plan = datafusion_df.logical_plan()
        return self.to_pandas_df(plan)


if __name__ == "__main__":
    ctx = SqlOnPandasContext()
    ctx.register_parquet("taxi", "/mnt/bigdata/nyctaxi/yellow_tripdata_2021-01.parquet")
    df = ctx.sql("select passenger_count from taxi")
    print(df)
