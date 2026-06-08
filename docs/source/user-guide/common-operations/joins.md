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

# Joins

DataFusion supports the following join variants via the method [`join`][datafusion.dataframe.DataFrame.join]

- Inner Join
- Left Join
- Right Join
- Full Join
- Left Semi Join
- Left Anti Join

For the examples in this section we'll use the following two DataFrames

```python exec="1" source="material-block" result="text" session="joins"
ctx = SessionContext()

left = ctx.from_pydict(
    {
        "customer_id": [1, 2, 3],
        "customer": ["Alice", "Bob", "Charlie"],
    }
)

right = ctx.from_pylist(
    [
        {"id": 1, "name": "CityCabs"},
        {"id": 2, "name": "MetroRide"},
        {"id": 5, "name": "UrbanGo"},
    ]
)
```


## Inner Join

When using an inner join, only rows containing the common values between the two join columns present in both DataFrames
will be included in the resulting DataFrame.

```python exec="1" source="material-block" result="text" session="joins"
print(left.join(right, left_on="customer_id", right_on="id", how="inner"))
```


The parameter `join_keys` specifies the columns from the left DataFrame and right DataFrame that contains the values
that should match.

## Left Join

A left join combines rows from two DataFrames using the key columns. It returns all rows from the left DataFrame and
matching rows from the right DataFrame. If there's no match in the right DataFrame, it returns null
values for the corresponding columns.

```python exec="1" source="material-block" result="text" session="joins"
print(left.join(right, left_on="customer_id", right_on="id", how="left"))
```


## Full Join

A full join merges rows from two tables based on a related column, returning all rows from both tables, even if there
is no match. Unmatched rows will have null values.

```python exec="1" source="material-block" result="text" session="joins"
print(left.join(right, left_on="customer_id", right_on="id", how="full"))
```


## Left Semi Join

A left semi join retrieves matching rows from the left table while
omitting duplicates with multiple matches in the right table.

```python exec="1" source="material-block" result="text" session="joins"
print(left.join(right, left_on="customer_id", right_on="id", how="semi"))
```


## Left Anti Join

A left anti join shows all rows from the left table without any matching rows in the right table,
based on a the specified matching columns. It excludes rows from the left table that have at least one matching row in
the right table.

```python exec="1" source="material-block" result="text" session="joins"
print(left.join(right, left_on="customer_id", right_on="id", how="anti"))
```


## Duplicate Keys

It is common to join two DataFrames on a common column name. Starting in
version 51.0.0, `` datafusion-python` `` will now coalesce on column with identical names by
default. This reduces problems with ambiguous column selection after joins.
You can disable this feature by setting the parameter `coalesce_duplicate_keys`
to `False`.

```python exec="1" source="material-block" result="text" session="joins"
left = ctx.from_pydict(
    {
        "id": [1, 2, 3],
        "customer": ["Alice", "Bob", "Charlie"],
    }
)

right = ctx.from_pylist(
    [
        {"id": 1, "name": "CityCabs"},
        {"id": 2, "name": "MetroRide"},
        {"id": 5, "name": "UrbanGo"},
    ]
)

print(left.join(right, "id", how="inner"))
```


In contrast to the above example, if we wish to get both columns:

```python exec="1" source="material-block" result="text" session="joins"
print(left.join(right, "id", how="inner", coalesce_duplicate_keys=False))
```


## Disambiguating Columns with `DataFrame.col()`

When both DataFrames contain non-key columns with the same name, you can use
[`col`][datafusion.dataframe.DataFrame.col] on each DataFrame **before** the
join to create fully qualified column references. These references can then be
used in the join predicate and when selecting from the result.

This is especially useful with [`join_on`][datafusion.dataframe.DataFrame.join_on],
which accepts expression-based predicates.

```python exec="1" source="material-block" result="text" session="joins"
left = ctx.from_pydict(
    {
        "id": [1, 2, 3],
        "val": [10, 20, 30],
    }
)

right = ctx.from_pydict(
    {
        "id": [1, 2, 3],
        "val": [40, 50, 60],
    }
)

joined = left.join_on(right, left.col("id") == right.col("id"), how="inner")

print(joined.select(left.col("id"), left.col("val"), right.col("val")))
```
