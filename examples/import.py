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
import pandas as pd
import polars as pl


# Create a context
ctx = datafusion.SessionContext()

# Create a datafusion DataFrame from a Python dictionary
# The dictionary keys represent column names and the dictionary values
# represent column values
df = ctx.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
assert type(df) == datafusion.DataFrame
# Dataframe:
# +---+---+
# | a | b |
# +---+---+
# | 1 | 4 |
# | 2 | 5 |
# | 3 | 6 |
# +---+---+

# Create a datafusion DataFrame from a Python list of rows
df = ctx.from_pylist([{"a": 1, "b": 4}, {"a": 2, "b": 5}, {"a": 3, "b": 6}])
assert type(df) == datafusion.DataFrame

# Convert pandas DataFrame to datafusion DataFrame
pandas_df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
df = ctx.from_pandas(pandas_df)
assert type(df) == datafusion.DataFrame

# Convert polars DataFrame to datafusion DataFrame
polars_df = pl.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
df = ctx.from_polars(polars_df)
assert type(df) == datafusion.DataFrame

# Convert Arrow Table to datafusion DataFrame
arrow_table = pa.Table.from_pydict({"a": [1, 2, 3], "b": [4, 5, 6]})
df = ctx.from_arrow_table(arrow_table)
assert type(df) == datafusion.DataFrame
