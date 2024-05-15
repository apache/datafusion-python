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

====================
DataFusion in Python
====================

This is a Python library that binds to `Apache Arrow <https://arrow.apache.org/>`_ in-memory query engine `DataFusion <https://github.com/apache/datafusion>`_.

Like pyspark, it allows you to build a plan through SQL or a DataFrame API against in-memory data, parquet or CSV files, run it in a multi-threaded environment, and obtain the result back in Python.

It also allows you to use UDFs and UDAFs for complex operations.

The major advantage of this library over other execution engines is that this library achieves zero-copy between Python and its execution engine: there is no cost in using UDFs, UDAFs, and collecting the results to Python apart from having to lock the GIL when running those operations.

Its query engine, DataFusion, is written in `Rust <https://www.rust-lang.org>`_, which makes strong assumptions about thread safety and lack of memory leaks.

Technically, zero-copy is achieved via the `c data interface <https://arrow.apache.org/docs/format/CDataInterface.html>`_.

Install
-------

.. code-block:: shell

    pip install datafusion

Example
-------

.. ipython:: python

    import datafusion
    from datafusion import col
    import pyarrow

    # create a context
    ctx = datafusion.SessionContext()

    # create a RecordBatch and a new DataFrame from it
    batch = pyarrow.RecordBatch.from_arrays(
        [pyarrow.array([1, 2, 3]), pyarrow.array([4, 5, 6])],
        names=["a", "b"],
    )
    df = ctx.create_dataframe([[batch]], name="batch_array")

    # create a new statement
    df = df.select(
        col("a") + col("b"),
        col("a") - col("b"),
    )

    df


.. _toc.links:
.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: LINKS

   Github and Issue Tracker <https://github.com/apache/datafusion-python>
   Rust's API Docs <https://docs.rs/datafusion/latest/datafusion/>
   Code of conduct <https://github.com/apache/datafusion/blob/main/CODE_OF_CONDUCT.md>
   Examples <https://github.com/apache/datafusion-python/tree/main/examples>

.. _toc.guide:
.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: USER GUIDE

   user-guide/introduction
   user-guide/basics
   user-guide/configuration
   user-guide/common-operations/index
   user-guide/io/index
   user-guide/sql


.. _toc.contributor_guide:
.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: CONTRIBUTOR GUIDE

   contributor-guide/introduction

.. _toc.api:
.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: API

   api
