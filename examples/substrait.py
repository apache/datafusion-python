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

import os
from datafusion import SessionContext
from datafusion import substrait as ss

# Create a DataFusion context
ctx = SessionContext()

# Get the directory of this script
current_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(current_dir, "testing", "data", "csv", "aggregate_test_100.csv")

try:
    # Register table with context using cross-platform path
    ctx.register_csv("aggregate_test_data", csv_path)
except Exception as e:
    print(f"Error registering CSV file: {e}")
    print(f"Please ensure the CSV file exists at: {csv_path}")
    raise

# Create a Substrait plan from SQL query
try:
    substrait_plan = ss.Serde.serialize_to_plan("SELECT * FROM aggregate_test_data", ctx)
except Exception as e:
    print(f"Error creating Substrait plan: {e}")
    raise

# Encode the plan to bytes
try:
    substrait_bytes = substrait_plan.encode()
except Exception as e:
    print(f"Error encoding Substrait plan: {e}")
    raise

# Alternative serialization approach
try:
    substrait_bytes = ss.Serde.serialize_bytes("SELECT * FROM aggregate_test_data", ctx)
except Exception as e:
    print(f"Error in alternative serialization: {e}")
    raise

# Deserialize the bytes back to a Substrait plan
try:
    substrait_plan = ss.Serde.deserialize_bytes(substrait_bytes)
except Exception as e:
    print(f"Error deserializing Substrait plan: {e}")
    raise

# Convert Substrait plan to DataFusion logical plan
try:
    df_logical_plan = ss.Consumer.from_substrait_plan(ctx, substrait_plan)
except Exception as e:
    print(f"Error converting to logical plan: {e}")
    raise

# Convert back to Substrait plan for demonstration
try:
    substrait_plan = ss.Producer.to_substrait_plan(df_logical_plan, ctx)
except Exception as e:
    print(f"Error converting back to Substrait plan: {e}")
    raise
