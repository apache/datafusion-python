# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datafusion import SessionContext
from datafusion import substrait as ss

# Create a DataFusion context
ctx = SessionContext()

# Register table with context
ctx.register_csv("aggregate_test_data", "./testing/data/csv/aggregate_test_100.csv")

substrait_plan = ss.Serde.serialize_to_plan("SELECT * FROM aggregate_test_data", ctx)
# type(substrait_plan) -> <class 'datafusion.substrait.plan'>

# Encode it to bytes
substrait_bytes = substrait_plan.encode()
# type(substrait_bytes) -> <class 'bytes'>, at this point the bytes can be distributed to file, network, etc safely
# where they could subsequently be deserialized on the receiving end.

# Alternative serialization approaches
# type(substrait_bytes) -> <class 'bytes'>, at this point the bytes can be distributed to file, network, etc safely
# where they could subsequently be deserialized on the receiving end.
substrait_bytes = ss.Serde.serialize_bytes("SELECT * FROM aggregate_test_data", ctx)

# Imagine here bytes would be read from network, file, etc ... for example brevity this is omitted and variable is simply reused
# type(substrait_plan) -> <class 'datafusion.substrait.plan'>
substrait_plan = ss.Serde.deserialize_bytes(substrait_bytes)

# type(df_logical_plan) -> <class 'substrait.LogicalPlan'>
df_logical_plan = ss.Consumer.from_substrait_plan(ctx, substrait_plan)

# Back to Substrait Plan just for demonstration purposes
# type(substrait_plan) -> <class 'datafusion.substrait.plan'>
substrait_plan = ss.Producer.to_substrait_plan(df_logical_plan, ctx)
