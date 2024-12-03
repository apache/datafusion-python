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

.. _user_guide_data_sources:

Data Sources
============

DataFusion provides a wide variety of ways to get data into a DataFrame to perform operations.

Local file
----------

DataFusion has the abilty to read from a variety of popular file formats, such as :ref:`Parquet <io_parquet>`,
:ref:`CSV <io_csv>`, :ref:`JSON <io_json>`, and :ref:`AVRO <io_avro>`.

.. ipython:: python

    from datafusion import SessionContext
    ctx = SessionContext()
    df = ctx.read_csv("pokemon.csv")
    df.show()

Create in-memory
----------------

Sometimes it can be convenient to create a small DataFrame from a Python list or dictionary object.
To do this in DataFusion, you can use one of the three functions
:py:func:`~datafusion.context.SessionContext.from_pydict`,
:py:func:`~datafusion.context.SessionContext.from_pylist`, or
:py:func:`~datafusion.context.SessionContext.create_dataframe`.

As their names suggest, ``from_pydict`` and ``from_pylist`` will create DataFrames from Python
dictionary and list objects, respectively. ``create_dataframe`` assumes you will pass in a list
of list of `PyArrow Record Batches <https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html>`_.

The following three examples all will create identical DataFrames:

.. ipython:: python

    import pyarrow as pa

    ctx.from_pylist([
        { "a": 1, "b": 10.0, "c": "alpha" },
        { "a": 2, "b": 20.0, "c": "beta" },
        { "a": 3, "b": 30.0, "c": "gamma" },
    ]).show()

    ctx.from_pydict({
        "a": [1, 2, 3],
        "b": [10.0, 20.0, 30.0],
        "c": ["alpha", "beta", "gamma"],
    }).show()

    batch = pa.RecordBatch.from_arrays(
        [
            pa.array([1, 2, 3]),
            pa.array([10.0, 20.0, 30.0]),
            pa.array(["alpha", "beta", "gamma"]),
        ],
        names=["a", "b", "c"],
    )

    ctx.create_dataframe([[batch]]).show()


Object Store
------------

DataFusion has support for multiple storage options in addition to local files.
The example below requires an appropriate S3 account with access credentials.

Supported Object Stores are

- :py:class:`~datafusion.object_store.AmazonS3`
- :py:class:`~datafusion.object_store.GoogleCloud`
- :py:class:`~datafusion.object_store.Http`
- :py:class:`~datafusion.object_store.LocalFileSystem`
- :py:class:`~datafusion.object_store.MicrosoftAzure`

.. code-block:: python

    from datafusion.object_store import AmazonS3

    region = "us-east-1"
    bucket_name = "yellow-trips"

    s3 = AmazonS3(
        bucket_name=bucket_name,
        region=region,
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    path = f"s3://{bucket_name}/"
    ctx.register_object_store("s3://", s3, None)

    ctx.register_parquet("trips", path)

    ctx.table("trips").show()

Other DataFrame Libraries
-------------------------

DataFusion can import DataFrames directly from other libraries, such as
`Polars <https://pola.rs/>`_ and `Pandas <https://pandas.pydata.org/>`_.
Since DataFusion version 42.0.0, any DataFrame library that supports the Arrow FFI PyCapsule
interface can be imported to DataFusion using the
:py:func:`~datafusion.context.SessionContext.from_arrow` function. Older verions of Polars may
not support the arrow interface. In those cases, you can still import via the
:py:func:`~datafusion.context.SessionContext.from_polars` function.

.. code-block:: python

    import pandas as pd

    data = { "a": [1, 2, 3], "b": [10.0, 20.0, 30.0], "c": ["alpha", "beta", "gamma"] }
    pandas_df = pd.DataFrame(data)

    datafusion_df = ctx.from_arrow(pandas_df)
    datafusion_df.show()

.. code-block:: python

    import polars as pl
    polars_df = pl.DataFrame(data)

    datafusion_df = ctx.from_arrow(polars_df)
    datafusion_df.show()

Delta Lake
----------

DataFusion 43.0.0 and later support the ability to register table providers from sources such
as Delta Lake. This will require a recent version of
`deltalake <https://delta-io.github.io/delta-rs/>`_ to provide the required interfaces.

.. code-block:: python

    from deltalake import DeltaTable

    delta_table = DeltaTable("path_to_table")
    ctx.register_table_provider("my_delta_table", delta_table)
    df = ctx.table("my_delta_table")
    df.show()

On older versions of ``deltalake`` (prior to 0.22) you can use the 
`Arrow DataSet <https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html>`_
interface to import to DataFusion, but this does not support features such as filter push down
which can lead to a significant performance difference.

.. code-block:: python

    from deltalake import DeltaTable

    delta_table = DeltaTable("path_to_table")
    ctx.register_dataset("my_delta_table", delta_table.to_pyarrow_dataset())
    df = ctx.table("my_delta_table")
    df.show()

Iceberg
-------

Coming soon!

Custom Table Provider
---------------------

You can implement a custom Data Provider in Rust and expose it to DataFusion through the
the interface as describe in the :ref:`Custom Table Provider <io_custom_table_provider>`
section. This is an advanced topic, but a
`user example <https://github.com/apache/datafusion-python/tree/main/examples/ffi-table-provider>`_
is provided in the DataFusion repository.
