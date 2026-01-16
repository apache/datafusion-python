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
``TableProvider`` or ``TableFunction``` via FFI must now provide access to a
``LogicalExtensionCodec`` and a ``TaskContextProvider``. The most convenient
way to provide these is from the ``datafusion-python`` ``SessionContext`` Python
object. The ``SessionContext`` now has a method to export a
``FFI_LogicalExtensionCodec``, which can satisfy this new requirement.

A complete example can be found in the `FFI example <https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-example>`_.
The constructor for your provider needs to take an an input the ``SessionContext``
python object. Instead of calling ``FFI_CatalogProvider::new`` you can use the
added method ``FFI_CatalogProvider::new_with_ffi_codec`` as follows:

.. code-block:: rust

    #[pymethods]
    impl MyCatalogProvider {
        #[new]
        pub fn new(session: &Bound<PyAny>) -> PyResult<Self> {
            let logical_codec = ffi_logical_codec_from_pycapsule(session)?;
            let inner = Arc::new(MemoryCatalogProvider::new());

            Ok(Self {
                inner,
                logical_codec,
            })
        }

        pub fn __datafusion_catalog_provider__<'py>(
            &self,
            py: Python<'py>,
        ) -> PyResult<Bound<'py, PyCapsule>> {
            let name = cr"datafusion_catalog_provider".into();
            let codec = self.logical_codec.clone();
            let catalog_provider =
                FFI_CatalogProvider::new_with_ffi_codec(Arc::new(self.clone()), None, codec);

            PyCapsule::new(py, catalog_provider, Some(name))
        }
    }

To extract the logical extension codec FFI object from the ``SessionContext`` you
can implement a helper method such as:

.. code-block:: rust

    pub(crate) fn ffi_logical_codec_from_pycapsule(
        obj: &Bound<PyAny>,
    ) -> PyResult<FFI_LogicalExtensionCodec> {
        let attr_name = "__datafusion_logical_extension_codec__";

        if obj.hasattr(attr_name)? {
            let capsule = obj.getattr(attr_name)?.call0()?;
            let capsule = capsule.downcast::<PyCapsule>()?;
            validate_pycapsule(capsule, "datafusion_logical_extension_codec")?;

            let provider = unsafe { capsule.reference::<FFI_LogicalExtensionCodec>() };

            Ok(provider.clone())
        } else {
            Err(PyValueError::new_err(
                "Expected PyCapsule object for FFI_LogicalExtensionCodec, but attribute does not exist",
            ))
        }
    }

The DataFusion FFI interface updates no longer depend directly on the
``datafusion`` core crate. You can improve your build times and potentially
reduce your library binary size by removing this dependency and instead
using the specific datafusion project crates.

For example, instead of including expressions like:

.. code-block: rust

    use datafusion::catalog::MemTable;

Instead you can now write:

.. code-block: rust

    use datafusion_catalog::MemTable;
