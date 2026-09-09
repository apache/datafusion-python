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

(extension_providers)=

# Providers and catalogs

Five hooks expose data to a session, at four levels of granularity. All five
follow the {ref}`capsule protocol <extension_capsule_protocol>` and all five
receive a {ref}`codec source <extension_getter_argument>` as their single
argument. Every one of them is implemented in [`datafusion-ffi-example`].

| Hook | Exposes | Registered with |
| --- | --- | --- |
| `__datafusion_table_provider__` | one table | {py:meth}`~datafusion.SessionContext.register_table` |
| `__datafusion_table_provider_factory__` | a factory that builds tables from `CREATE EXTERNAL TABLE` | {py:meth}`~datafusion.SessionContext.register_table_factory` |
| `__datafusion_schema_provider__` | a named set of tables | {py:meth}`datafusion.catalog.Catalog.register_schema` |
| `__datafusion_catalog_provider__` | a named set of schemas | {py:meth}`~datafusion.SessionContext.register_catalog_provider` |
| `__datafusion_catalog_provider_list__` | the whole catalog namespace | {py:meth}`~datafusion.SessionContext.register_catalog_provider_list` |

Start with a table provider. Reach for the schema and catalog levels when your
data source has its own namespace that should be browsable rather than
registered table by table, and for the provider list only when your library is
replacing the catalog namespace outright.

## A table provider

Implement
[TableProvider](https://datafusion.apache.org/library-user-guide/custom-table-providers.html)
in Rust, then expose it:

```rust
#[pymethods]
impl MyTableProvider {
    fn __datafusion_table_provider__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let provider = Arc::new(self.clone());
        let codec = ffi_logical_codec_from_pycapsule(session, None)?;
        let provider = FFI_TableProvider::new_with_ffi_codec(provider, false, None, codec);

        PyCapsule::new_with_value(py, provider, cr"datafusion_table_provider")
    }
}
```

Your users then register it as they would any other table — see
{ref}`io_custom_table_provider` for the Python side.

## The catalog family

The three catalog-level hooks have the same shape as each other. Taking the
schema provider as the representative:

```rust
#[pymethods]
impl MySchemaProvider {
    fn __datafusion_schema_provider__<'py>(
        &self,
        py: Python<'py>,
        codec: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let provider = Arc::clone(&self.inner) as Arc<dyn SchemaProvider + Send>;

        let codec = ffi_logical_codec_from_pycapsule(codec, None)?;
        let provider = FFI_SchemaProvider::new_with_ffi_codec(provider, None, codec);

        PyCapsule::new_with_value(py, provider, cr"datafusion_schema_provider")
    }
}
```

Swap `Schema` for `Catalog` or `CatalogProviderList` and the getter, capsule
name, and FFI type change together, following {ref}`the naming rule
<extension_capsule_protocol>`. `catalog_provider.rs` in
[`datafusion-ffi-example`] implements all three in one file, which is the
easiest way to see the symmetry.

Note the parameter name. These four hooks are handed the host's logical codec
directly rather than a session, so naming it `codec` is more honest than
`session` — but do not rely on either: pass it to
`ffi_logical_codec_from_pycapsule` and do not inspect it. See
{ref}`extension_getter_argument`.

## A table provider factory

A factory backs `CREATE EXTERNAL TABLE`: DataFusion hands it the statement's
options and it produces a provider. The getter takes the codec and wraps an
`Arc<dyn TableProviderFactory>`:

```rust
fn __datafusion_table_provider_factory__<'py>(
    &self,
    py: Python<'py>,
    codec: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyCapsule>> {
    let codec = ffi_logical_codec_from_pycapsule(codec, None)?;
    let factory = Arc::clone(&self.inner) as Arc<dyn TableProviderFactory + Send>;
    let factory = FFI_TableProviderFactory::new_with_ffi_codec(factory, None, codec);

    PyCapsule::new_with_value(py, factory, cr"datafusion_table_provider_factory")
}
```

`FFI_TableProviderFactory` carries no version field, so a factory is one of the
three components that cannot be version-checked on import. Be correspondingly
careful about which DataFusion version you build against.

## Serializing what you expose

If plans referencing your tables have to leave the process — a distributed
engine will make them — your library also needs a logical extension codec, so
the provider can be rebuilt on the other side. That is {doc}`codecs`, and
{doc}`bundles` is how you ship the two together.

The codec you pass to `new_with_ffi_codec` above is the **host's**, used to
serialize the parts of a plan the host owns. It is not a substitute for
contributing your own.

[`datafusion-ffi-example`]: https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-example
