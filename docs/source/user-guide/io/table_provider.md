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

(io_custom_table_provider)=

# Custom Table Provider

If you have a custom data source that you want to integrate with DataFusion, you can do so by
implementing the [TableProvider](https://datafusion.apache.org/library-user-guide/custom-table-providers.html)
interface in Rust and then exposing it in Python. To do so,
you must use DataFusion 43.0.0 or later and expose a [FFI_TableProvider](https://crates.io/crates/datafusion-ffi)
via [PyCapsule](https://pyo3.rs/main/doc/pyo3/types/struct.pycapsule).

A complete example can be found in the [examples folder](https://github.com/apache/datafusion-python/tree/main/examples).
For how to write one — the getter, what it receives, and how to serialize what
it exposes — see {ref}`extension_providers` in the Extension Guide.

Once you have this library available, you can construct a
{py:class}`~datafusion.Table` in Python and register it with the
`SessionContext`.

```python
from datafusion import SessionContext, Table

ctx = SessionContext()
provider = MyTableProvider()

ctx.register_table("capsule_table", provider)

ctx.table("capsule_table").show()
```
