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

from typing import TYPE_CHECKING

import datafusion as dfn
import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from datafusion import Catalog, SessionContext, Table, udtf

if TYPE_CHECKING:
    from datafusion.catalog import CatalogProvider, CatalogProviderExportable


# Note we take in `database` as a variable even though we don't use
# it because that will cause the fixture to set up the context with
# the tables we need.
def test_basic(ctx, database):
    with pytest.raises(KeyError):
        ctx.catalog("non-existent")

    default = ctx.catalog()
    assert default.names() == {"public"}

    for db in [default.schema("public"), default.schema()]:
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


def create_dataset() -> Table:
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    dataset = ds.dataset([batch])
    return Table(dataset)


class CustomSchemaProvider(dfn.catalog.SchemaProvider):
    def __init__(self):
        self.tables = {"table1": create_dataset()}

    def table_names(self) -> set[str]:
        return set(self.tables.keys())

    def register_table(self, name: str, table: Table):
        self.tables[name] = table

    def deregister_table(self, name, cascade: bool = True):
        del self.tables[name]

    def table(self, name: str) -> Table | None:
        return self.tables[name]

    def table_exist(self, name: str) -> bool:
        return name in self.tables


class CustomCatalogProvider(dfn.catalog.CatalogProvider):
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


class CustomCatalogProviderList(dfn.catalog.CatalogProviderList):
    def __init__(self):
        self.catalogs = {"my_catalog": CustomCatalogProvider()}

    def catalog_names(self) -> set[str]:
        return set(self.catalogs.keys())

    def catalog(self, name: str) -> Catalog | None:
        return self.catalogs[name]

    def register_catalog(
        self, name: str, catalog: CatalogProviderExportable | CatalogProvider | Catalog
    ) -> None:
        self.catalogs[name] = catalog


def test_python_catalog_provider_list(ctx: SessionContext):
    ctx.register_catalog_provider_list(CustomCatalogProviderList())

    # Ensure `datafusion` catalog does not exist since
    # we replaced the catalog list
    assert ctx.catalog_names() == {"my_catalog"}

    # Ensure registering works
    ctx.register_catalog_provider("second_catalog", CustomCatalogProvider())
    assert ctx.catalog_names() == {"my_catalog", "second_catalog"}


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


def test_in_memory_providers(ctx: SessionContext):
    catalog = dfn.catalog.Catalog.memory_catalog()
    ctx.register_catalog_provider("in_mem_catalog", catalog)

    assert ctx.catalog_names() == {"datafusion", "in_mem_catalog"}

    schema = dfn.catalog.Schema.memory_schema()
    catalog.register_schema("in_mem_schema", schema)

    schema.register_table("my_table", create_dataset())

    batches = ctx.sql("select * from in_mem_catalog.in_mem_schema.my_table").collect()

    assert len(batches) == 1
    assert batches[0].column(0) == pa.array([1, 2, 3])
    assert batches[0].column(1) == pa.array([4, 5, 6])


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


def test_schema_register_table_with_pyarrow_dataset(ctx: SessionContext):
    schema = ctx.catalog().schema()
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )
    dataset = ds.dataset([batch])
    table_name = "pa_dataset"

    try:
        schema.register_table(table_name, dataset)
        assert table_name in schema.table_names()

        result = ctx.sql(f"SELECT a, b FROM {table_name}").collect()

        assert len(result) == 1
        assert result[0].column(0) == pa.array([1, 2, 3])
        assert result[0].column(1) == pa.array([4, 5, 6])
    finally:
        schema.deregister_table(table_name)


def test_in_end_to_end_python_providers(ctx: SessionContext):
    """Test registering all python providers and running a query against them."""

    all_catalog_names = [
        "datafusion",
        "custom_catalog",
        "in_mem_catalog",
    ]

    all_schema_names = [
        "custom_schema",
        "in_mem_schema",
    ]

    ctx.register_catalog_provider(all_catalog_names[1], CustomCatalogProvider())
    ctx.register_catalog_provider(
        all_catalog_names[2], dfn.catalog.Catalog.memory_catalog()
    )

    for catalog_name in all_catalog_names:
        catalog = ctx.catalog(catalog_name)

        # Clean out previous schemas if they exist so we can start clean
        for schema_name in catalog.schema_names():
            catalog.deregister_schema(schema_name, cascade=False)

        catalog.register_schema(all_schema_names[0], CustomSchemaProvider())
        catalog.register_schema(all_schema_names[1], dfn.catalog.Schema.memory_schema())

        for schema_name in all_schema_names:
            schema = catalog.schema(schema_name)

            for table_name in schema.table_names():
                schema.deregister_table(table_name)

            schema.register_table("test_table", create_dataset())

    for catalog_name in all_catalog_names:
        for schema_name in all_schema_names:
            table_full_name = f"{catalog_name}.{schema_name}.test_table"

            batches = ctx.sql(f"select * from {table_full_name}").collect()

            assert len(batches) == 1
            assert batches[0].column(0) == pa.array([1, 2, 3])
            assert batches[0].column(1) == pa.array([4, 5, 6])


def test_register_python_function_as_udtf(ctx: SessionContext):
    basic_table = Table(ctx.sql("SELECT 3 AS value"))

    @udtf("my_table_function")
    def my_table_function_udtf() -> Table:
        return basic_table

    ctx.register_udtf(my_table_function_udtf)

    result = ctx.sql("SELECT * FROM my_table_function()").collect()
    assert len(result) == 1
    assert len(result[0]) == 1
    assert len(result[0][0]) == 1
    assert result[0][0][0].as_py() == 3
