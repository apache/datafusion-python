.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. _io_custom_table_provider:

Custom Table Provider
=====================

If you have a custom data source that you want to integrate with DataFusion, you can do so by
implementing the `TableProvider <https://datafusion.apache.org/library-user-guide/custom-table-providers.html>`_
interface in Rust and then exposing it in Python. To do so,
you must use DataFusion 43.0.0 or later and expose a `FFI_TableProvider <https://crates.io/crates/datafusion-ffi>`_
via `PyCapsule <https://pyo3.rs/main/doc/pyo3/types/struct.pycapsule>`_.

A complete example can be found in the `examples folder <https://github.com/apache/datafusion-python/tree/main/examples>`_.

.. code-block:: rust

    #[pymethods]
    impl MyTableProvider {

        fn __datafusion_table_provider__<'py>(
            &self,
            py: Python<'py>,
        ) -> PyResult<Bound<'py, PyCapsule>> {
            let name = cr"datafusion_table_provider".into();

            let provider = Arc::new(self.clone());
            let provider = FFI_TableProvider::new(provider, false, None);

            PyCapsule::new_bound(py, provider, Some(name.clone()))
        }
    }

Once you have this library available, you can construct a
:py:class:`~datafusion.Table` in Python and register it with the
``SessionContext``.

.. code-block:: python

    from datafusion import SessionContext, Table

    ctx = SessionContext()
    provider = MyTableProvider()

    ctx.register_table("capsule_table", provider)

    ctx.table("capsule_table").show()
