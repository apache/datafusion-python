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

Arrow
=====

DataFusion implements the
`Apache Arrow PyCapsule interface <https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html>`_
for importing and exporting DataFrames with zero copy. With this feature, any Python
project that implements this interface can share data back and forth with DataFusion
with zero copy.

We can demonstrate using `pyarrow <https://arrow.apache.org/docs/python/index.html>`_.

Importing to DataFusion
-----------------------

Here we will create an Arrow table and import it to DataFusion.

To import an Arrow table, use :py:func:`datafusion.context.SessionContext.from_arrow`.
This will accept any Python object that implements
`__arrow_c_stream__ <https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html#arrowstream-export>`_
or `__arrow_c_array__ <https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html#arrowarray-export>`_
and returns a ``StructArray``. Common pyarrow sources you can use are:

- `Array <https://arrow.apache.org/docs/python/generated/pyarrow.Array.html>`_ (but it must return a Struct Array)
- `Record Batch <https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html>`_
- `Record Batch Reader <https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatchReader.html>`_
- `Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`_

.. ipython:: python

    from datafusion import SessionContext
    import pyarrow as pa

    data = {"a": [1, 2, 3], "b": [4, 5, 6]}
    table = pa.Table.from_pydict(data)

    ctx = SessionContext()
    df = ctx.from_arrow(table)
    df

Exporting from DataFusion
-------------------------

DataFusion DataFrames implement ``__arrow_c_stream__`` PyCapsule interface, so any
Python library that accepts these can import a DataFusion DataFrame directly.

.. warning::
    It is important to note that this will cause the DataFrame execution to happen, which may be
    a time consuming task. That is, you will cause a
    :py:func:`datafusion.dataframe.DataFrame.collect` operation call to occur.


.. ipython:: python

    df = df.select((col("a") * lit(1.5)).alias("c"), lit("df").alias("d"))
    pa.table(df)

