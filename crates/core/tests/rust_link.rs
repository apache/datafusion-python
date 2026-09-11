// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion_python::context::PySessionContext;
use pyo3::prelude::*;

// An integration test is a separate executable consuming the rlib. Exercise
// Python calls as well as Rust construction so linking must resolve Py_* symbols.
#[test]
fn rust_consumer_can_execute_python_bindings() -> PyResult<()> {
    Python::initialize();
    Python::attach(|py| {
        let context = Bound::new(py, PySessionContext::new(None, None)?)?;
        let dataframe = context.call_method1("sql_with_options", ("SELECT 1 AS value",))?;
        let count = dataframe.call_method0("count")?.extract::<usize>()?;
        assert_eq!(count, 1);
        Ok(())
    })
}
