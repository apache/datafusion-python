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

(extension_why_ffi)=

# Why FFI

The DataFusion in Python project is designed to allow users to extend its
functionality in a few core areas. Ideally many users would like to package
their extensions as a Python package and easily integrate that package with
this project. This page describes the problem that makes the obvious approach
fail, and the approach this project uses instead.

## The primary issue

Suppose you wish to use DataFusion and you have a custom data source that can
produce tables that can then be queried against, similar to how you can
register a {ref}`CSV <io_csv>` or {ref}`Parquet <io_parquet>` file. In
DataFusion terminology, you likely want to implement a
{ref}`Custom Table Provider <io_custom_table_provider>`. In an effort to make
your data source as performant as possible and to utilize the features of
DataFusion, you may decide to write your source in Rust and then expose it
through [PyO3](https://pyo3.rs) as a Python library.

At first glance, it may appear the best way to do this is to add the
`datafusion-python` crate as a dependency, provide a `PyTable`, and then to
register it with the `SessionContext`. Unfortunately, this will not work.

When you produce your code as a Python library and it needs to interact with
the DataFusion library, at the lowest level they communicate through an
Application Binary Interface (ABI). The acronym sounds similar to API
(Application Programming Interface), but it is distinctly different.

The ABI sets the standard for how these libraries can share data and functions
between each other. One of the key differences between Rust and other
programming languages is that Rust does not have a stable ABI. What this means
in practice is that if you compile a Rust library with one version of the
`rustc` compiler and I compile another library to interface with it but I use a
different version of the compiler, there is no guarantee the interface will be
the same.

In practice, this means that a Python library built with `datafusion-python` as
a Rust dependency will generally **not** be compatible with the DataFusion
Python package, even if they reference the same version of
`datafusion-python`. If you attempt to do this, it may work on your local
computer if you have built both packages with the same optimizations. This can
sometimes lead to a false expectation that the code will work, but it
frequently breaks the moment you try to use your package against the released
packages.

You can find more information about the Rust ABI in their
[online documentation](https://doc.rust-lang.org/reference/abi.html).

## The FFI approach

Rust supports interacting with other programming languages through its Foreign
Function Interface (FFI). The advantage of using the FFI is that it enables you
to write data structures and functions that have a stable ABI. That allows you
to use Rust code with C, Python, and other languages. In fact, the
[PyO3](https://pyo3.rs) library uses the FFI to share data and functions
between Python and Rust.

The approach we are taking in the DataFusion in Python project is to
incrementally expose more portions of the DataFusion project via FFI
interfaces. This allows users to write Rust code that does **not** require the
`datafusion-python` crate as a dependency, expose their code in Python via
PyO3, and have it interact with the DataFusion Python package.

Early adopters of this approach include
[delta-rs](https://delta-io.github.io/delta-rs/) who has adapted their Table
Provider for use in `datafusion-python` with only a few lines of code. Also,
the DataFusion Python project uses the existing definitions from
[Apache Arrow CStream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html)
to support importing **and** exporting tables. Any Python package that supports
reading the Arrow C Stream interface can work with DataFusion Python out of the
box! You can read more about working with Arrow sources in the
{ref}`Data Sources <user_guide_data_sources>` page.

To learn more about the Foreign Function Interface in Rust, the
[Rustonomicon](https://doc.rust-lang.org/nomicon/ffi.html) is a good resource.

## Inspiration from Arrow

DataFusion is built upon [Apache Arrow](https://arrow.apache.org/). The
canonical Python Arrow implementation,
[pyarrow](https://arrow.apache.org/docs/python/index.html), provides an
excellent way to share Arrow data between Python projects without performing
any copy operations on the data, using a well defined set of interfaces — see
their [stream interface](https://arrow.apache.org/docs/format/CStreamInterface.html).
The [Rust Arrow implementation](https://github.com/apache/arrow-rs) also
supports these `C` style definitions via the Foreign Function Interface. Beyond
transferring data, `pyarrow` goes one step further and makes the interfaces
themselves easy to share in Python, by exposing PyCapsules that contain the
expected functionality.

Two lessons we leverage from the Arrow project in DataFusion Python are:

- We reuse the existing Arrow FFI functionality wherever possible.
- We expose PyCapsules that contain an FFI stable struct.

You can learn more about PyCapsules from the official
[Python online documentation](https://docs.python.org/3/c-api/capsule.html).
PyCapsules have excellent support in PyO3 already; the
[PyO3 online documentation](https://pyo3.rs/main/doc/pyo3/types/struct.pycapsule)
is a good source for more details on using PyCapsules in Rust.

## If FFI does not yet cover what you need

:::{note}
Suppose you needed to expose some other features of DataFusion and you could
not wait for the upstream repository to implement the FFI approach we describe.
In this case you decide to create your dependency on the `datafusion-python`
crate instead.

As we discussed, this is not guaranteed to work across different compiler
versions and optimization levels. If you wish to go down this route, there are
two approaches we have identified you can use.

1. Re-export all of `datafusion-python` yourself with your extensions built in.
2. Carefully synchronize your software releases with the `datafusion-python` CI
   build system so that your libraries use the exact same compiler, features,
   and optimization level.

We currently do not recommend either of these approaches as they are difficult
to maintain over a long period. Additionally, they require a tight version
coupling between libraries.

The better path is to open an issue describing what you need exposed. The FFI
surface in the [datafusion-ffi] crate grows in response to these.
:::

[datafusion-ffi]: https://crates.io/crates/datafusion-ffi
