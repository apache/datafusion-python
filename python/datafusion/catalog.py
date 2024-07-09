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

from __future__ import annotations

import datafusion._internal as df_internal

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow


class Catalog:
    def __init__(self, catalog: df_internal.Catalog) -> None:
        self.catalog = catalog

    def names(self) -> list[str]:
        return self.catalog.names()

    def database(self, name: str = "public") -> Database:
        return Database(self.catalog.database(name))


class Database:
    def __init__(self, db: df_internal.Database) -> None:
        self.db = db

    def names(self) -> set[str]:
        return self.db.names()

    def table(self, name: str) -> Table:
        return Table(self.db.table(name))


class Table:
    def __init__(self, table: df_internal.Table) -> None:
        self.table = table

    def schema(self) -> pyarrow.Schema:
        return self.table.schema()

    @property
    def kind(self) -> str:
        return self.table.kind()
