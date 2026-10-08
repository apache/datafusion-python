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

(pyo3_guidelines)=

# PyO3 binding guidelines

Conventions for the `#[pyclass]` bindings in `crates/`. These are review
policy for this repository, not part of the extension protocol — an extension
library is free to shape its own classes differently.

(ffi_pyclass_mutability)=

## Class mutability

PyO3 bindings should present immutable wrappers whenever a struct stores shared
or interior-mutable state. In practice this means that any `#[pyclass]`
containing an `Arc<RwLock<_>>` or similar synchronized primitive must opt into
`#[pyclass(frozen)]` unless there is a compelling reason not to.

The execution context illustrates the preferred pattern. `PySessionContext` in
{file}`src/context.rs` stays frozen even though it shares mutable state
internally via `SessionContext`. This ensures PyO3 tracks borrows correctly
while Python-facing APIs clone the inner `SessionContext` or return new
wrappers instead of mutating the existing instance in place:

```rust
#[pyclass(from_py_object, frozen, name = "SessionContext", module = "datafusion", subclass)]
#[derive(Clone)]
pub struct PySessionContext {
    pub ctx: SessionContext,
}
```

Occasionally a type must remain mutable—for example when PyO3 attribute setters
need to update fields directly. In these rare cases add an inline justification
so reviewers and future contributors understand why `frozen` is unsafe to
enable. `DataTypeMap` in {file}`src/common/data_type.rs` includes such a
comment because PyO3 still needs to track field updates:

```rust
// TODO: This looks like this needs pyo3 tracking so leaving unfrozen for now
#[derive(Debug, Clone)]
#[pyclass(from_py_object, name = "DataTypeMap", module = "datafusion.common", subclass)]
pub struct DataTypeMap {
    #[pyo3(get, set)]
    pub arrow_type: PyDataType,
    #[pyo3(get, set)]
    pub python_type: PythonType,
    #[pyo3(get, set)]
    pub sql_type: SqlType,
}
```

When reviewers encounter a mutable `#[pyclass]` without a comment, they should
request an explanation or ask that `frozen` be added. Keeping these wrappers
frozen by default helps avoid subtle bugs stemming from PyO3's interior
mutability tracking.
