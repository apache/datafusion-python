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

Once you have this library available, you can construct a
:py:class:`~datafusion.Table` in Python and register it with the
``SessionContext``. Tables can be created either from the PyCapsule exposed by your
Rust provider or from an existing :py:class:`~datafusion.dataframe.DataFrame`.
Call the provider's ``__datafusion_table_provider__()`` method to obtain the capsule
before constructing a ``Table``. The ``Table.from_view()`` helper is
deprecated; instead use ``Table.from_dataframe()`` or ``DataFrame.into_view()``.

.. note::

   :py:meth:`~datafusion.context.SessionContext.register_table_provider` is
   deprecated. Use
   :py:meth:`~datafusion.context.SessionContext.register_table` with the
   resulting :py:class:`~datafusion.Table` instead.

.. code-block:: python

    from datafusion import SessionContext, Table

    ctx = SessionContext()
    provider = MyTableProvider()

    capsule = provider.__datafusion_table_provider__()
    capsule_table = Table.from_capsule(capsule)

    df = ctx.from_pydict({"a": [1]})
    view_table = Table.from_dataframe(df)
    # or: view_table = df.into_view()

    ctx.register_table("capsule_table", capsule_table)
    ctx.register_table("view_table", view_table)

    ctx.table("capsule_table").show()
    ctx.table("view_table").show()

Both ``Table.from_capsule()`` and ``Table.from_dataframe()`` create
table providers that can be registered with the SessionContext using ``register_table()``.
