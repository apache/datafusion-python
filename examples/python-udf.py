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

import pyarrow
from datafusion import udf, SessionContext, functions as f


def is_null(array: pyarrow.Array) -> pyarrow.Array:
    return array.is_null()


is_null_arr = udf(is_null, [pyarrow.int64()], pyarrow.bool_(), "stable")

# create a context
ctx = SessionContext()

# create a RecordBatch and a new DataFrame from it
batch = pyarrow.RecordBatch.from_arrays(
    [pyarrow.array([1, 2, 3]), pyarrow.array([4, 5, 6])],
    names=["a", "b"],
)
df = ctx.create_dataframe([[batch]])

df = df.select(is_null_arr(f.col("a")))

result = df.collect()[0]

assert result.column(0) == pyarrow.array([False] * 3)
