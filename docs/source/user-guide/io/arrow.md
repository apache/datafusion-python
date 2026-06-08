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

# Arrow

DataFusion implements the
[Apache Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html)
for importing and exporting DataFrames with zero copy. With this feature, any Python
project that implements this interface can share data back and forth with DataFusion
with zero copy.

We can demonstrate using [pyarrow](https://arrow.apache.org/docs/python/index.html).

## Importing to DataFusion

Here we will create an Arrow table and import it to DataFusion.

To import an Arrow table, use [`from_arrow`][datafusion.context.SessionContext.from_arrow].
This will accept any Python object that implements
[\_\_arrow_c_stream\_\_](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html#arrowstream-export)
or [\_\_arrow_c_array\_\_](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html#arrowarray-export)
and returns a `StructArray`. Common pyarrow sources you can use are:

- [Array](https://arrow.apache.org/docs/python/generated/pyarrow.Array.html) (but it must return a Struct Array)
- [Record Batch](https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html)
- [Record Batch Reader](https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatchReader.html)
- [Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html)

```python exec="1" source="material-block" result="text" session="arrow"
import pyarrow as pa

data = {"a": [1, 2, 3], "b": [4, 5, 6]}
table = pa.Table.from_pydict(data)

ctx = SessionContext()
df = ctx.from_arrow(table)
print(df)
```


## Exporting from DataFusion

DataFusion DataFrames implement `__arrow_c_stream__` PyCapsule interface, so any
Python library that accepts these can import a DataFusion DataFrame directly.

Invoking `__arrow_c_stream__` triggers execution of the underlying query, but
batches are yielded incrementally rather than materialized all at once in memory.
Consumers can process the stream as it arrives. The stream executes lazily,
letting downstream readers pull batches on demand.

```python exec="1" source="material-block" result="text" session="arrow"
df = df.select((col("a") * lit(1.5)).alias("c"), lit("df").alias("d"))
print(pa.table(df))
```
