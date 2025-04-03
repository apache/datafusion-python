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

import datafusion
import pyarrow as pa
from datafusion import col

# create a context
ctx = datafusion.SessionContext()

# create a RecordBatch and a new DataFrame from it
batch = pa.RecordBatch.from_arrays(
    [pa.array([1, 2, 3]), pa.array([4, 5, 6])],
    names=["a", "b"],
)
df = ctx.create_dataframe([[batch]])

# create a new statement
df = df.select(
    col("a") + col("b"),
    col("a") - col("b"),
)

# execute and collect the first (and only) batch
result = df.collect()[0]

assert result.column(0) == pa.array([5, 7, 9])
assert result.column(1) == pa.array([-3, -3, -3])
