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


# create a context
ctx = datafusion.SessionContext()

# create a new datafusion DataFrame
df = ctx.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
# Dataframe:
# +---+---+
# | a | b |
# +---+---+
# | 1 | 4 |
# | 2 | 5 |
# | 3 | 6 |
# +---+---+

# export to pandas dataframe
pandas_df = df.to_pandas()
assert pandas_df.shape == (3, 2)

# export to PyArrow table
arrow_table = df.to_arrow_table()
assert arrow_table.shape == (3, 2)

# export to Polars dataframe
polars_df = df.to_polars()
assert polars_df.shape == (3, 2)

# export to Python list of rows
pylist = df.to_pylist()
assert pylist == [{"a": 1, "b": 4}, {"a": 2, "b": 5}, {"a": 3, "b": 6}]

# export to Pyton dictionary of columns
pydict = df.to_pydict()
assert pydict == {"a": [1, 2, 3], "b": [4, 5, 6]}
