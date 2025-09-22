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
            let name = CString::new("datafusion_table_provider").unwrap();

            let provider = Arc::new(self.clone());
            let provider = FFI_TableProvider::new(provider, false, None);

            PyCapsule::new_bound(py, provider, Some(name.clone()))
        }
    }

Once you have this library available, you can instantiate the Rust-backed
provider directly in Python and register it with the
:py:meth:`~datafusion.context.SessionContext.register_table` method.
Objects implementing ``__datafusion_table_provider__`` are accepted as-is, so
there is no need to build a Python ``TableProvider`` wrapper just to integrate
with DataFusion.

When you need to register a DataFusion
:py:class:`~datafusion.dataframe.DataFrame`, call
:py:meth:`~datafusion.dataframe.DataFrame.into_view` to obtain an in-memory
view.  This is equivalent to the legacy ``TableProvider.from_dataframe()``
helper.

.. note::

   :py:meth:`~datafusion.context.SessionContext.register_table_provider` is
   deprecated. Use
   :py:meth:`~datafusion.context.SessionContext.register_table` with the
   provider instance or view returned by
   :py:meth:`~datafusion.dataframe.DataFrame.into_view` instead.

.. code-block:: python

    from datafusion import SessionContext

    ctx = SessionContext()
    provider = MyTableProvider()

    df = ctx.from_pydict({"a": [1]})
    view_provider = df.into_view()

    ctx.register_table("provider_table", provider)
    ctx.register_table("view_table", view_provider)

    ctx.table("provider_table").show()
    ctx.table("view_table").show()

The capsule-based helpers remain available for advanced integrations that need
to manipulate FFI objects explicitly.  ``TableProvider.from_capsule()`` continues
to wrap an ``FFI_TableProvider`` (and will stay available as a compatibility
alias if it is renamed in :issue:`1`), while ``TableProvider.from_dataframe()``
simply forwards to :py:meth:`DataFrame.into_view` for convenience.
