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

import pyarrow as pa
import pytest


# Note we take in `database` as a variable even though we don't use
# it because that will cause the fixture to set up the context with
# the tables we need.
def test_basic(ctx, database):
    with pytest.raises(KeyError):
        ctx.catalog("non-existent")

    default = ctx.catalog()
    assert default.names() == ["public"]

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
