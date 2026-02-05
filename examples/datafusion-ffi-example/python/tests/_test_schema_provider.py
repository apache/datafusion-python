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

import pyarrow as pa
import pyarrow.dataset as ds
import pytest
from datafusion import SessionContext, Table
from datafusion.catalog import Schema
from datafusion_ffi_example import FixedSchemaProvider, MyCatalogProvider


def create_test_dataset() -> Table:
    """Create a simple test dataset."""
    batch = pa.RecordBatch.from_arrays(
        [pa.array([100, 200, 300]), pa.array([1.1, 2.2, 3.3])],
        names=["id", "value"],
    )
    dataset = ds.dataset([batch])
    return Table(dataset)


@pytest.mark.parametrize("inner_capsule", [True, False])
def test_schema_provider_extract_values(inner_capsule: bool) -> None:
    ctx = SessionContext()

    my_schema_name = "my_schema"

    schema_provider = FixedSchemaProvider()
    if inner_capsule:
        schema_provider = schema_provider.__datafusion_schema_provider__(ctx)

    ctx.catalog().register_schema(my_schema_name, schema_provider)

    expected_schema_name = "my_schema"
    expected_table_name = "my_table"
    expected_table_columns = ["units", "price"]

    default_catalog = ctx.catalog()

    catalog_schemas = default_catalog.names()
    assert expected_schema_name in catalog_schemas
    my_schema = default_catalog.schema(expected_schema_name)
    assert expected_table_name in my_schema.names()
    my_table = my_schema.table(expected_table_name)
    assert expected_table_columns == my_table.schema.names

    result = ctx.table(f"{expected_schema_name}.{expected_table_name}").collect()
    assert len(result) == 2

    col0_result = [r.column(0) for r in result]
    col1_result = [r.column(1) for r in result]
    expected_col0 = [
        pa.array([10, 20, 30], type=pa.int32()),
        pa.array([5, 7], type=pa.int32()),
    ]
    expected_col1 = [
        pa.array([1, 2, 5], type=pa.float64()),
        pa.array([1.5, 2.5], type=pa.float64()),
    ]
    assert col0_result == expected_col0
    assert col1_result == expected_col1


def test_ffi_schema_provider_basic():
    """Test basic FFI SchemaProvider functionality."""
    ctx = SessionContext()

    # Register FFI schema
    schema_provider = FixedSchemaProvider()
    ctx.catalog().register_schema("ffi_schema", schema_provider)

    # Verify the schema exists
    schema = ctx.catalog().schema("ffi_schema")
    table_names = schema.names()
    assert "my_table" in table_names

    # Query the pre-populated table
    result = ctx.sql("SELECT * FROM ffi_schema.my_table").collect()
    assert len(result) == 2
    assert result[0].num_columns == 2


def test_ffi_schema_provider_register_table():
    """Test registering additional tables to FFI SchemaProvider."""
    ctx = SessionContext()

    schema_provider = FixedSchemaProvider()
    ctx.catalog().register_schema("ffi_schema", schema_provider)

    schema = ctx.catalog().schema("ffi_schema")

    # Register a new table
    schema.register_table("additional_table", create_test_dataset())

    # Verify the table was registered
    assert "additional_table" in schema.names()

    # Query the new table
    result = ctx.sql("SELECT * FROM ffi_schema.additional_table").collect()
    assert len(result) == 1
    assert result[0].column(0) == pa.array([100, 200, 300])
    assert result[0].column(1) == pa.array([1.1, 2.2, 3.3])


def test_ffi_schema_provider_deregister_table():
    """Test deregistering tables from FFI SchemaProvider."""
    ctx = SessionContext()

    schema_provider = FixedSchemaProvider()
    ctx.catalog().register_schema("ffi_schema", schema_provider)

    schema = ctx.catalog().schema("ffi_schema")

    # Register two tables
    schema.register_table("temp_table1", create_test_dataset())
    schema.register_table("temp_table2", create_test_dataset())

    # Verify both exist
    names = schema.names()
    assert "temp_table1" in names
    assert "temp_table2" in names

    # Deregister one table
    schema.deregister_table("temp_table1")

    # Verify it's gone
    names = schema.names()
    assert "temp_table1" not in names
    assert "temp_table2" in names


def test_mixed_ffi_and_python_providers():
    """Test mixing FFI and Python providers in the same catalog/schema."""
    ctx = SessionContext()

    # Register FFI catalog
    ffi_catalog = MyCatalogProvider()
    ctx.register_catalog_provider("ffi_catalog", ffi_catalog)

    # Register Python memory schema to FFI catalog
    python_schema = Schema.memory_schema()
    ctx.catalog("ffi_catalog").register_schema("python_schema", python_schema)

    # Add table to Python schema
    python_schema.register_table("python_table", create_test_dataset())

    # Query both FFI table and Python table
    result_ffi = ctx.sql("SELECT * FROM ffi_catalog.my_schema.my_table").collect()
    assert len(result_ffi) == 2

    result_python = ctx.sql(
        "SELECT * FROM ffi_catalog.python_schema.python_table"
    ).collect()
    assert len(result_python) == 1
    assert result_python[0].column(0) == pa.array([100, 200, 300])


def test_ffi_catalog_with_multiple_schemas():
    """Test FFI catalog with multiple schemas of different types."""
    ctx = SessionContext()

    catalog_provider = MyCatalogProvider()
    ctx.register_catalog_provider("multi_catalog", catalog_provider)

    catalog = ctx.catalog("multi_catalog")

    # Register different types of schemas
    ffi_schema = FixedSchemaProvider()
    memory_schema = Schema.memory_schema()

    catalog.register_schema("ffi_schema", ffi_schema)
    catalog.register_schema("memory_schema", memory_schema)

    # Add tables to memory schema
    memory_schema.register_table("mem_table", create_test_dataset())

    # Verify all schemas exist
    names = catalog.names()
    assert "my_schema" in names  # Pre-populated
    assert "ffi_schema" in names
    assert "memory_schema" in names

    # Query tables from each schema
    result = ctx.sql("SELECT * FROM multi_catalog.my_schema.my_table").collect()
    assert len(result) == 2

    result = ctx.sql("SELECT * FROM multi_catalog.ffi_schema.my_table").collect()
    assert len(result) == 2

    result = ctx.sql("SELECT * FROM multi_catalog.memory_schema.mem_table").collect()
    assert len(result) == 1
    assert result[0].column(0) == pa.array([100, 200, 300])


def test_ffi_schema_table_exist():
    """Test table_exist method on FFI SchemaProvider."""
    ctx = SessionContext()

    schema_provider = FixedSchemaProvider()
    ctx.catalog().register_schema("ffi_schema", schema_provider)

    schema = ctx.catalog().schema("ffi_schema")

    # Check pre-populated table
    assert schema.table_exist("my_table")

    # Check non-existent table
    assert not schema.table_exist("nonexistent_table")

    # Register a new table and check
    schema.register_table("new_table", create_test_dataset())
    assert schema.table_exist("new_table")

    # Deregister and check
    schema.deregister_table("new_table")
    assert not schema.table_exist("new_table")
