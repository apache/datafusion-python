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

Arrow Interface
===============

DataFusion DataFrames can stream results to Arrow-based Python libraries
without first materializing all batches in memory. This page focuses on the
Arrow interfaces exposed by ``DataFrame`` and when to use each one.

Zero-copy Streaming
-------------------

DataFusion DataFrames implement the ``__arrow_c_stream__`` protocol, enabling
zero-copy, lazy streaming into Arrow-based Python libraries. With the streaming
protocol, batches are produced on demand.

.. note::

    The protocol is implementation-agnostic and works with any Python library
    that understands the Arrow C streaming interface (for example, PyArrow
    or other Arrow-compatible implementations). The sections below provide a
    short PyArrow-specific example and general guidance for other
    implementations.

PyArrow
-------

.. code-block:: python

    import pyarrow as pa

    # Create a PyArrow RecordBatchReader without materializing all batches
    reader = pa.RecordBatchReader.from_stream(df)
    for batch in reader:
        ...  # process each batch as it is produced

DataFrames are also iterable, yielding :class:`datafusion.RecordBatch`
objects lazily so you can loop over results directly without importing
PyArrow:

.. code-block:: python

    for batch in df:
        ...  # each batch is a ``datafusion.RecordBatch``

Each batch exposes ``to_pyarrow()``, allowing conversion to a PyArrow
table. ``pa.table(df)`` collects the entire DataFrame eagerly into a
PyArrow table:

.. code-block:: python

    import pyarrow as pa

    table = pa.table(df)

Asynchronous iteration is supported as well, allowing integration with
``asyncio`` event loops:

.. code-block:: python

    async for batch in df:
        ...  # process each batch as it is produced

Execute as Stream
-----------------

For finer control over streaming execution, use
:py:meth:`~datafusion.DataFrame.execute_stream` to obtain a
:py:class:`datafusion.RecordBatchStream`:

.. code-block:: python

    stream = df.execute_stream()
    for batch in stream:
        ...  # process each batch as it is produced

.. tip::

    To get a PyArrow reader instead, call

    ``pa.RecordBatchReader.from_stream(df)``.

When partition boundaries are important,
:py:meth:`~datafusion.DataFrame.execute_stream_partitioned`
returns an iterable of :py:class:`datafusion.RecordBatchStream` objects, one per
partition:

.. code-block:: python

    for stream in df.execute_stream_partitioned():
        for batch in stream:
            ...  # each stream yields RecordBatches

To process partitions concurrently, first collect the streams into a list
and then poll each one in a separate ``asyncio`` task:

.. code-block:: python

    import asyncio

    async def consume(stream):
        async for batch in stream:
            ...

    streams = list(df.execute_stream_partitioned())
    await asyncio.gather(*(consume(s) for s in streams))

See :doc:`../io/arrow` for additional details on the Arrow interface.
