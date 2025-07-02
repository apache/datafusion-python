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

Python Extensions
=================

The DataFusion in Python project is designed to allow users to extend its functionality in a few core
areas. Ideally many users would like to package their extensions as a Python package and easily
integrate that package with this project. This page serves to describe some of the challenges we face
when doing these integrations and the approach our project uses.

The Primary Issue
-----------------

Suppose you wish to use DataFusion and you have a custom data source that can produce tables that
can then be queried against, similar to how you can register a :ref:`CSV <io_csv>` or
:ref:`Parquet <io_parquet>` file. In DataFusion terminology, you likely want to implement a 
:ref:`Custom Table Provider <io_custom_table_provider>`. In an effort to make your data source
as performant as possible and to utilize the features of DataFusion, you may decide to write
your source in Rust and then expose it through `PyO3 <https://pyo3.rs>`_ as a Python library.

At first glance, it may appear the best way to do this is to add the ``datafusion-python``
crate as a dependency, provide a ``PyTable``, and then to register it with the 
``SessionContext``. Unfortunately, this will not work.

When you produce your code as a Python library and it needs to interact with the DataFusion
library, at the lowest level they communicate through an Application Binary Interface (ABI).
The acronym sounds similar to API (Application Programming Interface), but it is distinctly
different.

The ABI sets the standard for how these libraries can share data and functions between each
other. One of the key differences between Rust and other programming languages is that Rust
does not have a stable ABI. What this means in practice is that if you compile a Rust library
with one version of the ``rustc`` compiler and I compile another library to interface with it
but I use a different version of the compiler, there is no guarantee the interface will be
the same.

In practice, this means that a Python library built with ``datafusion-python`` as a Rust
dependency will generally **not** be compatible with the DataFusion Python package, even
if they reference the same version of ``datafusion-python``. If you attempt to do this, it may
work on your local computer if you have built both packages with the same optimizations.
This can sometimes lead to a false expectation that the code will work, but it frequently
breaks the moment you try to use your package against the released packages.

You can find more information about the Rust ABI in their
`online documentation <https://doc.rust-lang.org/reference/abi.html>`_.

The FFI Approach
----------------

Rust supports interacting with other programming languages through it's Foreign Function
Interface (FFI). The advantage of using the FFI is that it enables you to write data structures
and functions that have a stable ABI. The allows you to use Rust code with C, Python, and
other languages. In fact, the `PyO3 <https://pyo3.rs>`_ library uses the FFI to share data
and functions between Python and Rust.

The approach we are taking in the DataFusion in Python project is to incrementally expose
more portions of the DataFusion project via FFI interfaces. This allows users to write Rust
code that does **not** require the ``datafusion-python`` crate as a dependency, expose their
code in Python via PyO3, and have it interact with the DataFusion Python package.

Early adopters of this approach include `delta-rs <https://delta-io.github.io/delta-rs/>`_
who has adapted their Table Provider for use in ```datafusion-python``` with only a few lines
of code. Also, the DataFusion Python project uses the existing definitions from
`Apache Arrow CStream Interface <https://arrow.apache.org/docs/format/CStreamInterface.html>`_
to support importing **and** exporting tables. Any Python package that supports reading
the Arrow C Stream interface can work with DataFusion Python out of the box! You can read
more about working with Arrow sources in the :ref:`Data Sources <user_guide_data_sources>`
page.

To learn more about the Foreign Function Interface in Rust, the
`Rustonomicon <https://doc.rust-lang.org/nomicon/ffi.html>`_ is a good resource.

Inspiration from Arrow
----------------------

DataFusion is built upon `Apache Arrow <https://arrow.apache.org/>`_. The canonical Python
Arrow implementation, `pyarrow <https://arrow.apache.org/docs/python/index.html>`_ provides
an excellent way to share Arrow data between Python projects without performing any copy
operations on the data. They do this by using a well defined set of interfaces. You can
find the details about their stream interface
`here <https://arrow.apache.org/docs/format/CStreamInterface.html>`_. The
`Rust Arrow Implementation <https://github.com/apache/arrow-rs>`_ also supports these
``C`` style definitions via the Foreign Function Interface.

In addition to using these interfaces to transfer Arrow data between libraries, ``pyarrow``
goes one step further to make sharing the interfaces easier in Python. They do this
by exposing PyCapsules that contain the expected functionality.

You can learn more about PyCapsules from the official
`Python online documentation <https://docs.python.org/3/c-api/capsule.html>`_. PyCapsules
have excellent support in PyO3 already. The
`PyO3 online documentation <https://pyo3.rs/main/doc/pyo3/types/struct.pycapsule>`_ is a good source
for more details on using PyCapsules in Rust.

Two lessons we leverage from the Arrow project in DataFusion Python are:

- We reuse the existing Arrow FFI functionality wherever possible.
- We expose PyCapsules that contain a FFI stable struct.

Implementation Details
----------------------

The bulk of the code necessary to perform our FFI operations is in the upstream 
`DataFusion <https://datafusion.apache.org/>`_ core repository. You can review the code and
documentation in the `datafusion-ffi`_ crate.

Our FFI implementation is narrowly focused at sharing data and functions with Rust backed
libraries. This allows us to use the `abi_stable crate <https://crates.io/crates/abi_stable>`_.
This is an excellent crate that allows for easy conversion between Rust native types
and FFI-safe alternatives. For example, if you needed to pass a ``Vec<String>`` via FFI,
you can simply convert it to a ``RVec<RString>`` in an intuitive manner. It also supports
features like ``RResult`` and ``ROption`` that do not have an obvious translation to a
C equivalent.

The `datafusion-ffi`_ crate has been designed to make it easy to convert from DataFusion
traits into their FFI counterparts. For example, if you have defined a custom
`TableProvider <https://docs.rs/datafusion/45.0.0/datafusion/catalog/trait.TableProvider.html>`_
and you want to create a sharable FFI counterpart, you could write:

.. code-block:: rust

    let my_provider = MyTableProvider::default();
    let ffi_provider = FFI_TableProvider::new(Arc::new(my_provider), false, None);

If you were interfacing with a library that provided the above ``FFI_TableProvider`` and
you needed to turn it back into an ``TableProvider``, you can turn it into a
``ForeignTableProvider`` with implements the ``TableProvider`` trait.

.. code-block:: rust

    let foreign_provider: ForeignTableProvider = ffi_provider.into();

If you review the code in `datafusion-ffi`_ you will find that each of the traits we share
across the boundary has two portions, one with a ``FFI_`` prefix and one with a ``Foreign``
prefix. This is used to distinguish which side of the FFI boundary that struct is
designed to be used on. The structures with the ``FFI_`` prefix are to be used on the
**provider** of the structure. In the example we're showing, this means the code that has
written the underlying ``TableProvider`` implementation to access your custom data source.
The structures with the ``Foreign`` prefix are to be used by the receiver. In this case,
it is the ``datafusion-python`` library.

In order to share these FFI structures, we need to wrap them in some kind of Python object
that can be used to interface from one package to another. As described in the above
section on our inspiration from Arrow, we use ``PyCapsule``. We can create a ``PyCapsule``
for our provider thusly:

.. code-block:: rust

    let name = CString::new("datafusion_table_provider")?;
    let my_capsule = PyCapsule::new_bound(py, provider, Some(name))?;

On the receiving side, turn this pycapsule object into the ``FFI_TableProvider``, which
can then be turned into a ``ForeignTableProvider`` the associated code is:

.. code-block:: rust

    let capsule = capsule.downcast::<PyCapsule>()?;
    let provider = unsafe { capsule.reference::<FFI_TableProvider>() };

By convention the ``datafusion-python`` library expects a Python object that has a
``TableProvider`` PyCapsule to have this capsule accessible by calling a function named
``__datafusion_table_provider__``. You can see a complete working example of how to
share a ``TableProvider`` from one python library to DataFusion Python in the
`repository examples folder <https://github.com/apache/datafusion-python/tree/main/examples/datafusion-ffi-example>`_.

This section has been written using ``TableProvider`` as an example. It is the first
extension that has been written using this approach and the most thoroughly implemented.
As we continue to expose more of the DataFusion features, we intend to follow this same
design pattern.

Alternative Approach
--------------------

Suppose you needed to expose some other features of DataFusion and you could not wait
for the upstream repository to implement the FFI approach we describe. In this case
you decide to create your dependency on the ``datafusion-python`` crate instead.

As we discussed, this is not guaranteed to work across different compiler versions and
optimization levels. If you wish to go down this route, there are two approaches we
have identified you can use.

#. Re-export all of ``datafusion-python`` yourself with your extensions built in.
#. Carefully synchonize your software releases with the ``datafusion-python`` CI build
   system so that your libraries use the exact same compiler, features, and
   optimization level.

We currently do not recommend either of these approaches as they are difficult to
maintain over a long period. Additionally, they require a tight version coupling
between libraries.

Status of Work
--------------

At the time of this writing, the FFI features are under active development. To see
the latest status, we recommend reviewing the code in the `datafusion-ffi`_ crate.

.. _datafusion-ffi: https://crates.io/crates/datafusion-ffi
