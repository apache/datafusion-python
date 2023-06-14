<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
# DataFusion Create Table

It's easy to create DataFusion tables from a variety of data sources.

## Create Table from Python Dictionary

Here's how to create a DataFusion table from a Python dictionary:

```python
from datafusion import SessionContext

ctx = SessionContext()

df = ctx.from_pydict({"a": [1, 2, 3, 1], "b": [4, 5, 6, 7]}, name="my_table")
```

Supplying the `name` parameter is optional.  You only need to name the table if you'd like to query it with the SQL API.

You can also create a DataFrame without a name that can be queried with the Python API:

```python
from datafusion import SessionContext

ctx = SessionContext()

df = ctx.from_pydict({"a": [1, 2, 3, 1], "b": [4, 5, 6, 7]})
```

## Create Table from CSV

You can read a CSV into a DataFusion DataFrame.  Here's how to read the `G1_1e8_1e2_0_0.csv` file into a table named `csv_1e8`:

```python
ctx.register_csv("csv_1e8", "G1_1e8_1e2_0_0.csv")
```

## Create Table from Parquet

You can read a Parquet file into a DataFusion DataFrame.  Here's how to read the `yellow_tripdata_2021-01.parquet` file into a table named `taxi`.

```python
ctx.register_table("taxi", "yellow_tripdata_2021-01.parquet")
```
