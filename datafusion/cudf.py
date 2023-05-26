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
import datafusion
from datafusion.context import BaseSessionContext
from datafusion.expr import Projection, TableScan, Column

from datafusion.datacontainer import (
    UDF,
    DataContainer,
    FunctionDescription,
    SchemaContainer,
    Statistics,
)

from datafusion.common import (
    SqlTable,
    SqlSchema,
)

from datafusion import input_utils
from datafusion.input_utils import InputType, InputUtil

logger = logging.getLogger(__name__)

class SessionContext(BaseSessionContext):

    DEFAULT_CATALOG_NAME = "datafusion"
    DEFAULT_SCHEMA_NAME = "root"

    def __init__(self, context=None, logging_level=logging.INFO):
        """
        Create a new Session.
        """
        self.context = context if not context else datafusion.SessionContext()

        # Set the logging level for this SQL context
        logging.basicConfig(level=logging_level)

        # Name of the root catalog
        self.catalog_name = self.DEFAULT_CATALOG_NAME
        # Name of the root schema
        self.schema_name = self.DEFAULT_SCHEMA_NAME
        # All schema information
        self.schema = {self.schema_name: SchemaContainer(self.schema_name)}

        self.context.register_schema(self.schema_name, SqlSchema(self.schema_name))

        # Register default `InputPlugins` that will be used to
        # understand and consume data in different formats
        InputUtil.add_plugin_class(input_utils.DataFrameInputPlugin, replace=False) # Existing cudf.DataFrame object
        InputUtil.add_plugin_class(input_utils.LocationInputPlugin, replace=False) # File location on disk

    def create_table(
        self,
        table_name: str,
        input_table: InputType,
        format: str = None,
        schema_name: str = None,
        statistics: Statistics = None,
        **kwargs,
    ):
        """
        Registering a cudf DataFrame/table makes it usable in SQL queries.
        The name you give here can be used as table name in the SQL later.

        Please note, that the table is stored as it is now.
        If you change the table later, you need to re-register.

        Example:
            This code registers a data frame as table "data"
            and then uses it in a query.

            .. code-block:: python

                c.create_table("data", df)
                df_result = c.sql("SELECT a, b FROM data")

            This code reads a file from disk.
            Please note that we assume that the file(s) are reachable under this path
            from every node in the cluster

            .. code-block:: python

                c.create_table("data", "/home/user/data.csv")
                df_result = c.sql("SELECT a, b FROM data")

        Args:
            table_name: (:obj:`str`): Under which name should the new table be addressable
            input_table (:class:`dask.dataframe.DataFrame` or :class:`pandas.DataFrame` or :obj:`str` or :class:`hive.Cursor`):
                The data frame/location/hive connection to register.
            format (:obj:`str`): Only used when passing a string into the ``input`` parameter.
                Specify the file format directly here if it can not be deduced from the extension.
                If set to "memory", load the data from a published dataset in the dask cluster.
            schema_name: (:obj:`str`): in which schema to create the table. By default, will use the currently selected schema.
            statistics: (:obj:`Statistics`): if given, use these statistics during the cost-based optimization.
            **kwargs: Additional arguments for specific formats. See :ref:`data_input` for more information.

        """
        logger.debug(
            f"Creating table: '{table_name}' of format type '{format}' in schema '{schema_name}'"
        )

        schema_name = schema_name or self.schema_name

        dc = InputUtil.to_dc(
            input_table,
            table_name=table_name,
            format=format,
            **kwargs,
        )

        if type(input_table) == str:
            dc.filepath = input_table
            self.schema[schema_name].filepaths[table_name.lower()] = input_table

        # TODO: Implement reading physical statistics
        dc.statistics = Statistics(float("nan"))

        self.schema[schema_name].tables[table_name.lower()] = dc
        self.schema[schema_name].statistics[table_name.lower()] = statistics

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

    def register_table(self, name, path, **kwargs):
        self.parquet_tables[name] = path
        self.datafusion_ctx.register_parquet(name, path)

    def explain(self, sql):
        super.explain()

    def sql(self, sql):
        datafusion_df = self.datafusion_ctx.sql(sql)
        plan = datafusion_df.logical_plan()
        return self.to_cudf_df(plan)
