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

from datafusion import udf, SessionContext
import pyarrow as pa


# Define a user-defined function (UDF)
def is_null(array: pa.Array) -> pa.Array:
    return array.is_null()


is_null_arr = udf(
    is_null,
    [pa.int64()],
    pa.bool_(),
    "stable",
    # This will be the name of the UDF in SQL
    # If not specified it will by default the same as Python function name
    name="is_null",
)

# Create a context
ctx = SessionContext()

# Create a datafusion DataFrame from a Python dictionary
ctx.from_pydict({"a": [1, 2, 3], "b": [4, None, 6]}, name="t")
# Dataframe:
# +---+---+
# | a | b |
# +---+---+
# | 1 | 4 |
# | 2 |   |
# | 3 | 6 |
# +---+---+

# Register UDF for use in SQL
ctx.register_udf(is_null_arr)

# Query the DataFrame using SQL
result_df = ctx.sql("select a, is_null(b) as b_is_null from t")
# Dataframe:
# +---+-----------+
# | a | b_is_null |
# +---+-----------+
# | 1 | false     |
# | 2 | true      |
# | 3 | false     |
# +---+-----------+
assert result_df.to_pydict()["b_is_null"] == [False, True, False]
