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
# DataFusion Quickstart

You can easily query a DataFusion table with the Python API or with pure SQL.

Let's create a small DataFrame and then run some queries with both APIs.

Start by creating a DataFrame with four rows of data and two columns: `a` and `b`.

```python
from datafusion import SessionContext

ctx = SessionContext()

df = ctx.from_pydict({"a": [1, 2, 3, 1], "b": [4, 5, 6, 7]}, name="my_table")
```

Let's append a column to this DataFrame that adds columns `a` and `b` with the SQL API.

```
ctx.sql("select a, b, a + b as sum_a_b from my_table")

+---+---+---------+
| a | b | sum_a_b |
+---+---+---------+
| 1 | 4 | 5       |
| 2 | 5 | 7       |
| 3 | 6 | 9       |
| 1 | 7 | 8       |
+---+---+---------+
```

DataFusion makes it easy to run SQL queries on DataFrames.

Now let's run the same query with the DataFusion Python API:

```python
from datafusion import col

df.select(
    col("a"),
    col("b"),
    col("a") + col("b"),
)
```

We get the same result as before:

```
+---+---+-------------------------+
| a | b | my_table.a + my_table.b |
+---+---+-------------------------+
| 1 | 4 | 5                       |
| 2 | 5 | 7                       |
| 3 | 6 | 9                       |
| 1 | 7 | 8                       |
+---+---+-------------------------+
```

DataFusion also allows you to query data with a well-designed Python interface.

Python users have two great ways to query DataFusion tables.
