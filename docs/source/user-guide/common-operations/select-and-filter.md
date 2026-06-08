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

# Column Selections

Use [`select`][datafusion.dataframe.DataFrame.select] for basic column selection.

DataFusion can work with several file types, to start simple we can use a subset of the
[TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page),
which you can download [here](https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet).

```python exec="1" source="material-block" result="text" session="select-and-filter"
ctx = SessionContext()
df = ctx.read_parquet("yellow_tripdata_2021-01.parquet")
print(df.select("trip_distance", "passenger_count"))
```


For mathematical or logical operations use [`col`][datafusion.col.col] to select columns, and give meaningful names to the resulting
operations using [`alias`][datafusion.expr.Expr.alias]

```python exec="1" source="material-block" result="text" session="select-and-filter"
print(df.select((col("tip_amount") + col("tolls_amount")).alias("tips_plus_tolls")))
```


<div class="admonition warning">
<p class="admonition-title">Warning</p>

Please be aware that all identifiers are effectively made lower-case in SQL, so if your file has capital letters
(ex: Name) you must put your column name in double quotes or the selection won’t work. As an alternative for simple
column selection use [`select`][datafusion.dataframe.DataFrame.select] without double quotes

</div>

For selecting columns with capital letters use `'"VendorID"'`

```python exec="1" source="material-block" result="text" session="select-and-filter"
print(df.select(col('"VendorID"')))
```


To combine it with literal values use the [`lit`][datafusion.lit]

```python exec="1" source="material-block" result="text" session="select-and-filter"
large_trip_distance = col("trip_distance") > lit(5.0)
low_passenger_count = col("passenger_count") < lit(4)
print(df.select((large_trip_distance & low_passenger_count).alias("lonely_trips")))
```
