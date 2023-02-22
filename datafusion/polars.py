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

import polars
import datafusion
from datafusion.expr import Projection, TableScan, Aggregate
from datafusion.expr import Column, AggregateFunction


class SessionContext:
    def __init__(self):
        self.datafusion_ctx = datafusion.SessionContext()
        self.parquet_tables = {}

    def register_parquet(self, name, path):
        self.parquet_tables[name] = path
        self.datafusion_ctx.register_parquet(name, path)

    def to_polars_expr(self, expr):
        # get Python wrapper for logical expression
        expr = expr.to_variant()

        if isinstance(expr, Column):
            return polars.col(expr.name())
        else:
            raise Exception("unsupported expression: {}".format(expr))

    def to_polars_df(self, plan):
        # recurse down first to translate inputs into Polars data frames
        inputs = [self.to_polars_df(x) for x in plan.inputs()]

        # get Python wrapper for logical operator node
        node = plan.to_variant()

        if isinstance(node, Projection):
            args = [self.to_polars_expr(expr) for expr in node.projections()]
            return inputs[0].select(*args)
        elif isinstance(node, Aggregate):
            groupby_expr = [
                self.to_polars_expr(expr) for expr in node.group_by_exprs()
            ]
            aggs = []
            for expr in node.aggregate_exprs():
                expr = expr.to_variant()
                if isinstance(expr, AggregateFunction):
                    if expr.aggregate_type() == "COUNT":
                        aggs.append(polars.count().alias("{}".format(expr)))
                    else:
                        raise Exception(
                            "Unsupported aggregate function {}".format(
                                expr.aggregate_type()
                            )
                        )
                else:
                    raise Exception(
                        "Unsupported aggregate function {}".format(expr)
                    )
            df = inputs[0].groupby(groupby_expr).agg(aggs)
            return df
        elif isinstance(node, TableScan):
            return polars.read_parquet(self.parquet_tables[node.table_name()])
        else:
            raise Exception(
                "unsupported logical operator: {}".format(type(node))
            )

    def sql(self, sql):
        datafusion_df = self.datafusion_ctx.sql(sql)
        plan = datafusion_df.logical_plan()
        return self.to_polars_df(plan)
