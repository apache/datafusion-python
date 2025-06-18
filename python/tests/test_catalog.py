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

import datafusion as dfn
import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from datafusion import SessionContext, Table


# Note we take in `database` as a variable even though we don't use
# it because that will cause the fixture to set up the context with
# the tables we need.
def test_basic(ctx, database):
    with pytest.raises(KeyError):
        ctx.catalog("non-existent")

    default = ctx.catalog()
    assert default.names() == {"public"}

    for db in [default.database("public"), default.database()]:
        assert db.names() == {"csv1", "csv", "csv2"}

    table = db.table("csv")
    assert table.kind == "physical"
    assert table.schema == pa.schema(
        [
            pa.field("int", pa.int64(), nullable=True),
            pa.field("str", pa.string(), nullable=True),
            pa.field("float", pa.float64(), nullable=True),
        ]
    )


class CustomTableProvider:
    def __init__(self):
        pass


def create_dataset() -> pa.dataset.Dataset:
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    return ds.dataset([batch])


class CustomSchemaProvider:
    def __init__(self):
        self.tables = {"table1": create_dataset()}

    def table_names(self) -> set[str]:
        return set(self.tables.keys())

    def register_table(self, name: str, table: Table):
        self.tables[name] = table

    def deregister_table(self, name, cascade: bool = True):
        del self.tables[name]


class CustomCatalogProvider:
    def __init__(self):
        self.schemas = {"my_schema": CustomSchemaProvider()}

    def schema_names(self) -> set[str]:
        return set(self.schemas.keys())

    def schema(self, name: str):
        return self.schemas[name]

    def register_schema(self, name: str, schema: dfn.catalog.Schema):
        self.schemas[name] = schema

    def deregister_schema(self, name, cascade: bool):
        del self.schemas[name]


def test_python_catalog_provider(ctx: SessionContext):
    ctx.register_catalog_provider("my_catalog", CustomCatalogProvider())

    # Check the default catalog provider
    assert ctx.catalog("datafusion").names() == {"public"}

    my_catalog = ctx.catalog("my_catalog")
    assert my_catalog.names() == {"my_schema"}

    my_catalog.register_schema("second_schema", CustomSchemaProvider())
    assert my_catalog.schema_names() == {"my_schema", "second_schema"}

    my_catalog.deregister_schema("my_schema")
    assert my_catalog.schema_names() == {"second_schema"}


def test_python_schema_provider(ctx: SessionContext):
    catalog = ctx.catalog()

    catalog.deregister_schema("public")

    catalog.register_schema("test_schema1", CustomSchemaProvider())
    assert catalog.names() == {"test_schema1"}

    catalog.register_schema("test_schema2", CustomSchemaProvider())
    catalog.deregister_schema("test_schema1")
    assert catalog.names() == {"test_schema2"}


def test_python_table_provider(ctx: SessionContext):
    catalog = ctx.catalog()

    catalog.register_schema("custom_schema", CustomSchemaProvider())
    schema = catalog.schema("custom_schema")

    assert schema.table_names() == {"table1"}

    schema.deregister_table("table1")
    schema.register_table("table2", create_dataset())
    assert schema.table_names() == {"table2"}

    # Use the default schema instead of our custom schema

    schema = catalog.schema()

    schema.register_table("table3", create_dataset())
    assert schema.table_names() == {"table3"}

    schema.deregister_table("table3")
    schema.register_table("table4", create_dataset())
    assert schema.table_names() == {"table4"}
