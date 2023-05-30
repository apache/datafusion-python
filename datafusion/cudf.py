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

import logging
import cudf
from datafusion.context import BaseSessionContext
from datafusion.expr import Projection, TableScan, Column

from datafusion.common import SqlSchema

logger = logging.getLogger(__name__)


class SessionContext(BaseSessionContext):
    def __init__(self, context, logging_level=logging.INFO):
        """
        Create a new Session.
        """
        # Cudf requires a provided context
        self.context = context

        # Set the logging level for this SQL context
        logging.basicConfig(level=logging_level)

        # Name of the root catalog
        self.catalog_name = self.DEFAULT_CATALOG_NAME
        # Name of the root schema
        self.schema_name = self.DEFAULT_SCHEMA_NAME
        # Add the schema to the context
        sch = SqlSchema(self.schema_name)
        self.schemas = {}
        self.schemas[self.schema_name] = sch
        self.context.register_schema(self.schema_name, sch)

    def to_cudf_expr(self, expr):
        # get Python wrapper for logical expression
        expr = expr.to_variant()

        if isinstance(expr, Column):
            return expr.name()
        else:
            raise Exception("unsupported expression: {}".format(expr))

    def to_cudf_df(self, plan):
        # recurse down first to translate inputs into pandas data frames
        inputs = [self.to_cudf_df(x) for x in plan.inputs()]

        # get Python wrapper for logical operator node
        node = plan.to_variant()

        if isinstance(node, Projection):
            args = [self.to_cudf_expr(expr) for expr in node.projections()]
            return inputs[0][args]
        elif isinstance(node, TableScan):
            return cudf.read_parquet(self.parquet_tables[node.table_name()])
        else:
            raise Exception(
                "unsupported logical operator: {}".format(type(node))
            )

    def create_schema(self, schema_name: str, **kwargs):
        logger.debug(f"Creating schema: {schema_name}")
        self.schemas[schema_name] = SqlSchema(schema_name)
        self.context.register_schema(schema_name, SqlSchema(schema_name))

    def update_schema(self, schema_name: str, new_schema: SqlSchema, **kwargs):
        self.schemas[schema_name] = new_schema

    def drop_schema(self, schema_name, **kwargs):
        del self.schemas[schema_name]

    def show_schemas(self, **kwargs):
        return self.schemas

    def register_table(self, name, path, **kwargs):
        self.parquet_tables[name] = path
        self.datafusion_ctx.register_parquet(name, path)

    def explain(self, sql):
        super.explain()

    def sql(self, sql):
        datafusion_df = self.datafusion_ctx.sql(sql)
        plan = datafusion_df.logical_plan()
        return self.to_cudf_df(plan)
