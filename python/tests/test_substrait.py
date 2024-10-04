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

from datafusion import SessionContext
from datafusion import substrait as ss
import pytest


@pytest.fixture
def ctx():
    return SessionContext()


def test_substrait_serialization(ctx):
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )

    ctx.register_record_batches("t", [[batch]])

    assert ctx.catalog().database().names() == {"t"}

    # For now just make sure the method calls blow up
    substrait_plan = ss.Serde.serialize_to_plan("SELECT * FROM t", ctx)
    substrait_bytes = substrait_plan.encode()
    assert isinstance(substrait_bytes, bytes)
    substrait_bytes = ss.Serde.serialize_bytes("SELECT * FROM t", ctx)
    substrait_plan = ss.Serde.deserialize_bytes(substrait_bytes)
    logical_plan = ss.Consumer.from_substrait_plan(ctx, substrait_plan)

    # demonstrate how to create a DataFrame from a deserialized logical plan
    df = ctx.create_dataframe_from_logical_plan(logical_plan)

    substrait_plan = ss.Producer.to_substrait_plan(df.logical_plan(), ctx)


@pytest.mark.parametrize("path_to_str", (True, False))
def test_substrait_file_serialization(ctx, tmp_path, path_to_str):
    batch = pa.RecordBatch.from_arrays(
        [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
        names=["a", "b"],
    )

    ctx.register_record_batches("t", [[batch]])

    assert ctx.catalog().database().names() == {"t"}

    path = tmp_path / "substrait_plan"
    path = str(path) if path_to_str else path

    sql_command = "SELECT * FROM T"
    ss.Serde.serialize(sql_command, ctx, path)

    expected_plan = ss.Serde.serialize_to_plan(sql_command, ctx)
    actual_plan = ss.Serde.deserialize(path)

    expected_logical_plan = ss.Consumer.from_substrait_plan(ctx, expected_plan)
    expected_actual_plan = ss.Consumer.from_substrait_plan(ctx, actual_plan)

    assert str(expected_logical_plan) == str(expected_actual_plan)
