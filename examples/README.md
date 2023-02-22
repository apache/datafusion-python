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

# DataFusion Python Examples

Some examples rely on data which can be downloaded from the following site:

- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Here is a direct link to the file used in the examples:

- https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet

### Executing Queries with DataFusion

- [Query a Parquet file using SQL](./sql-parquet.py)
- [Query a Parquet file using the DataFrame API](./dataframe-parquet.py)
- [Run a SQL query and store the results in a Pandas DataFrame](./sql-to-pandas.py)
- [Query PyArrow Data](./query-pyarrow-data.py)

### Running User-Defined Python Code

- [Register a Python UDF with DataFusion](./python-udf.py)
- [Register a Python UDAF with DataFusion](./python-udaf.py)

### Substrait Support

- [Serialize query plans using Substrait](./substrait.py)

### Executing SQL against DataFrame Libraries (Experimental)

- [Executing SQL on Polars](./sql-on-polars.py)
- [Executing SQL on Pandas](./sql-on-pandas.py)
- [Executing SQL on cuDF](./sql-on-cudf.py)
