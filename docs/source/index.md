```python exec="1" session="index"
import os
import pathlib

import datafusion  # noqa: F401
from datafusion import (  # noqa: F401
    SessionContext,
    col,
    column,
    lit,
    literal,
)
from datafusion import functions as f  # noqa: F401
from datafusion.dataframe_formatter import configure_formatter

# mkdocs runs from the repo root; the demo data lives at docs/source/.
for candidate in ("docs/source", ".."):
    p = pathlib.Path(candidate)
    if (p / "pokemon.csv").exists():
        os.chdir(p)
        break

configure_formatter(max_rows=10, show_truncation_message=False)
```

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

# DataFusion in Python

This is a Python library that binds to [Apache Arrow](https://arrow.apache.org/) in-memory query engine [DataFusion](https://github.com/apache/datafusion).

Like pyspark, it allows you to build a plan through SQL or a DataFrame API against in-memory data, parquet or CSV files, run it in a multi-threaded environment, and obtain the result back in Python.

It also allows you to use UDFs and UDAFs for complex operations.

The major advantage of this library over other execution engines is that this library achieves zero-copy between Python and its execution engine: there is no cost in using UDFs, UDAFs, and collecting the results to Python apart from having to lock the GIL when running those operations.

Its query engine, DataFusion, is written in [Rust](https://www.rust-lang.org), which makes strong assumptions about thread safety and lack of memory leaks.

Technically, zero-copy is achieved via the [c data interface](https://arrow.apache.org/docs/format/CDataInterface.html).

## Install

```shell
pip install datafusion
```

## Example

```python exec="1" source="material-block" result="text" session="index"
ctx = SessionContext()

df = ctx.read_csv("pokemon.csv")

df.show()
```
