# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from abc import ABC, abstractmethod
from typing import Any, Dict, List

from datafusion.common import SqlSchema, SqlTable


class BaseSessionContext(ABC):
    """
    Abstraction defining all methods, properties, and common functionality
    shared amongst implementations using DataFusion as their SQL Parser/Engine
    """

    DEFAULT_CATALOG_NAME = "root"
    DEFAULT_SCHEMA_NAME = "datafusion"

    @abstractmethod
    def create_schema(
        self,
        schema_name: str,
        **kwargs,
    ):
        """
        Creates/Registers a logical container that holds database
        objects such as tables, views, indexes, and other
        related objects. It provides a way to group related database
        objects together. A schema can be owned by a database
        user and can be used to separate objects in different
        logical groups for easy management.
        """
        pass

    @abstractmethod
    def update_schema(
        self,
        schema_name: str,
        new_schema: SqlSchema,
        **kwargs,
    ):
        """
        Updates an existing schema in the SessionContext
        """
        pass

    @abstractmethod
    def drop_schema(
        self,
        schema_name: str,
        **kwargs,
    ):
        """
        Drops the specified Schema, based on name, from the current context
        """
        pass

    @abstractmethod
    def show_schemas(self, **kwargs) -> Dict[str, SqlSchema]:
        """
        Return all schemas in the current SessionContext impl.
        """
        pass

    @abstractmethod
    def create_table(
        self,
        schema_name: str,
        table_name: str,
        input_source: Any,
        **kwargs,
    ):
        """
        Creates/Registers a table in the specied schema instance
        """
        pass

    @abstractmethod
    def update_table(
        self,
        schema_name: str,
        table_name: str,
        new_table: SqlTable,
        **kwargs,
    ):
        """
        Updates an existing table in the SessionContext
        """
        pass

    @abstractmethod
    def drop_table(
        self,
        schema_name: str,
        table_name: str,
        **kwargs,
    ):
        """
        Drops the specified table, based on name, from the current context
        """
        pass

    @abstractmethod
    def show_tables(self, **kwargs) -> List[SqlTable]:
        """
        Return all tables in the current SessionContext impl.
        """
        pass

    @abstractmethod
    def register_table(
        self,
        table_name: str,
        path: str,
        **kwargs,
    ):
        pass

    # TODO: Remove abstraction, this functionality can be shared
    # between all implementing classes since it just prints the
    # logical plan from DataFusion
    @abstractmethod
    def explain(self, sql):
        pass

    @abstractmethod
    def sql(self, sql):
        pass
