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
from datafusion_ffi_example import MyCatalogProvider


def create_test_dataset() -> Table:
    """Create a simple test dataset."""
    batch = pa.RecordBatch.from_arrays(
        [pa.array([100, 200, 300]), pa.array([1.1, 2.2, 3.3])],
        names=["id", "value"],
    )
    dataset = ds.dataset([batch])
    return Table(dataset)


@pytest.mark.parametrize("inner_capsule", [True, False])
def test_ffi_catalog_provider_basic(inner_capsule: bool) -> None:
    """Test basic FFI CatalogProvider functionality."""
    ctx = SessionContext()

    # Register FFI catalog
    catalog_provider = MyCatalogProvider()
    if inner_capsule:
        catalog_provider = catalog_provider.__datafusion_catalog_provider__(ctx)

    ctx.register_catalog_provider("ffi_catalog", catalog_provider)

    # Verify the catalog exists
    catalog = ctx.catalog("ffi_catalog")
    schema_names = catalog.names()
    assert "my_schema" in schema_names

    # Query the pre-populated table
    result = ctx.sql("SELECT * FROM ffi_catalog.my_schema.my_table").collect()
    assert len(result) == 2
    assert result[0].num_columns == 2


def test_ffi_catalog_provider_register_schema():
    """Test registering additional schemas to FFI CatalogProvider."""
    ctx = SessionContext()

    catalog_provider = MyCatalogProvider()
    ctx.register_catalog_provider("ffi_catalog", catalog_provider)

    catalog = ctx.catalog("ffi_catalog")

    # Register a new memory schema
    new_schema = Schema.memory_schema()
    catalog.register_schema("additional_schema", new_schema)

    # Verify the schema was registered
    assert "additional_schema" in catalog.names()

    # Add a table to the new schema
    new_schema.register_table("new_table", create_test_dataset())

    # Query the new table
    result = ctx.sql("SELECT * FROM ffi_catalog.additional_schema.new_table").collect()
    assert len(result) == 1
    assert result[0].column(0) == pa.array([100, 200, 300])


def test_ffi_catalog_provider_deregister_schema():
    """Test deregistering schemas from FFI CatalogProvider."""
    ctx = SessionContext()

    catalog_provider = MyCatalogProvider()
    ctx.register_catalog_provider("ffi_catalog", catalog_provider)

    catalog = ctx.catalog("ffi_catalog")

    # Register two schemas
    schema1 = Schema.memory_schema()
    schema2 = Schema.memory_schema()
    catalog.register_schema("temp_schema1", schema1)
    catalog.register_schema("temp_schema2", schema2)

    # Verify both exist
    names = catalog.names()
    assert "temp_schema1" in names
    assert "temp_schema2" in names

    # Deregister one schema
    catalog.deregister_schema("temp_schema1")

    # Verify it's gone
    names = catalog.names()
    assert "temp_schema1" not in names
    assert "temp_schema2" in names
