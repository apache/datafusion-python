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
from datafusion import SessionContext
from datafusion_ffi_example import MyCatalogProvider


def test_catalog_provider():
    ctx = SessionContext()

    my_catalog_name = "my_catalog"
    expected_schema_name = "my_schema"
    expected_table_name = "my_table"
    expected_table_columns = ["units", "price"]

    catalog_provider = MyCatalogProvider()
    ctx.register_catalog_provider(my_catalog_name, catalog_provider)
    my_catalog = ctx.catalog(my_catalog_name)

    my_catalog_schemas = my_catalog.names()
    assert expected_schema_name in my_catalog_schemas
    my_database = my_catalog.database(expected_schema_name)
    assert expected_table_name in my_database.names()
    my_table = my_database.table(expected_table_name)
    assert expected_table_columns == my_table.schema.names

    result = ctx.table(
        f"{my_catalog_name}.{expected_schema_name}.{expected_table_name}"
    ).collect()
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
