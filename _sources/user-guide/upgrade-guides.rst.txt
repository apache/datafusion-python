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

Upgrade Guides
==============

DataFusion 52.0.0
-----------------

This version includes a major update to the :ref:`ffi` due to upgrades
to the `Foreign Function Interface <https://doc.rust-lang.org/nomicon/ffi.html>`_.
Users who contribute their own ``CatalogProvider``, ``SchemaProvider``,
``TableProvider`` or ``TableFunction`` via FFI must now provide access to a
``LogicalExtensionCodec`` and a ``TaskContextProvider``. The function signatures
for the methods to get these ``PyCapsule`` objects now requires an additional
parameter, which is a Python object that can be used to extract the
``FFI_LogicalExtensionCodec`` that is necessary.

A complete example can be found in the `FFI example <https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-example>`_.
Your methods need to be updated to take an additional parameter like in this
example.

.. code-block:: rust

    #[pymethods]
    impl MyCatalogProvider {
        pub fn __datafusion_catalog_provider__<'py>(
            &self,
            py: Python<'py>,
            session: Bound<PyAny>,
        ) -> PyResult<Bound<'py, PyCapsule>> {
            let name = cr"datafusion_catalog_provider".into();

            let provider = Arc::clone(&self.inner) as Arc<dyn CatalogProvider + Send>;

            let codec = ffi_logical_codec_from_pycapsule(session)?;
            let provider = FFI_CatalogProvider::new_with_ffi_codec(provider, None, codec);

            PyCapsule::new(py, provider, Some(name))
        }
    }

To extract the logical extension codec FFI object from the provided object you
can implement a helper method such as:

.. code-block:: rust

    pub(crate) fn ffi_logical_codec_from_pycapsule(
        obj: Bound<PyAny>,
    ) -> PyResult<FFI_LogicalExtensionCodec> {
        let attr_name = "__datafusion_logical_extension_codec__";
        let capsule = if obj.hasattr(attr_name)? {
            obj.getattr(attr_name)?.call0()?
        } else {
            obj
        };

        let capsule = capsule.downcast::<PyCapsule>()?;
        validate_pycapsule(capsule, "datafusion_logical_extension_codec")?;

        let codec = unsafe { capsule.reference::<FFI_LogicalExtensionCodec>() };

        Ok(codec.clone())
    }


The DataFusion FFI interface updates no longer depend directly on the
``datafusion`` core crate. You can improve your build times and potentially
reduce your library binary size by removing this dependency and instead
using the specific datafusion project crates.

For example, instead of including expressions like:

.. code-block:: rust

    use datafusion::catalog::MemTable;

Instead you can now write:

.. code-block:: rust

    use datafusion_catalog::MemTable;
