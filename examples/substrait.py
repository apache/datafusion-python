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

# Get the directory of the current script
script_dir = os.path.dirname(os.path.abspath(__file__))

# Construct the path to the CSV file
# Using os.path.join for cross-platform compatibility
csv_file_path = os.path.join(script_dir, '..', 'testing', 'data', 'csv', 'aggregate_test_100.csv')

# Create a DataFusion context
ctx = SessionContext()

# Register table with context
ctx.register_csv("aggregate_test_data", "./testing/data/csv/aggregate_test_100.csv")
try:
    # Register table with context
    ctx.register_csv("aggregate_test_data", csv_file_path)
except Exception as e:
    print(f"Error registering CSV file: {e}")
    print(f"Looking for file at: {csv_file_path}")
    raise

# Create Substrait plan from SQL query
substrait_plan = ss.Serde.serialize_to_plan("SELECT * FROM aggregate_test_data", ctx)
# type(substrait_plan) -> <class 'datafusion.substrait.plan'>



