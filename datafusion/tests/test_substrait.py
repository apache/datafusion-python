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

    assert ctx.tables() == {"t"}

    # For now just make sure the method calls blow up
    substrait_plan = ss.substrait.serde.serialize_to_plan(
        "SELECT * FROM t", ctx
    )
    substrait_bytes = ss.substrait.serde.serialize_bytes(
        "SELECT * FROM t", ctx
    )
    substrait_plan = ss.substrait.serde.deserialize_bytes(substrait_bytes)
    logical_plan = ss.substrait.consumer.from_substrait_plan(
        ctx, substrait_plan
    )

    # demonstrate how to create a DataFrame from a deserialized logical plan
    df = ctx.create_dataframe_from_logical_plan(logical_plan)

    substrait_plan = ss.substrait.producer.to_substrait_plan(df.logical_plan())
