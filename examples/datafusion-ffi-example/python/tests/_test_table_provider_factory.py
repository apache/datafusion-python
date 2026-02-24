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
import pytest
from datafusion import SessionContext
from datafusion_ffi_example import MyTableProviderFactory


def test_table_provider_factory_ffi() -> None:
    ctx = SessionContext()
    table = MyTableProviderFactory()

    ctx.register_table_factory("MY_FORMAT", table)

    # Create a new external table
    ctx.sql("""
        CREATE EXTERNAL TABLE
        foo
        STORED AS my_format
        LOCATION '';
    """)

    # Query the pre-populated table
    result = ctx.sql("SELECT * FROM foo;").collect()
    assert len(result) == 2
    assert result[0].num_columns == 2
