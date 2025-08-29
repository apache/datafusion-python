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

Configuration
=============

Let's look at how we can configure DataFusion. When creating a :py:class:`~datafusion.context.SessionContext`, you can pass in
a :py:class:`~datafusion.context.SessionConfig` and :py:class:`~datafusion.context.RuntimeEnvBuilder` object. These two cover a wide range of options.

.. code-block:: python

    from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext

    # create a session context with default settings
    ctx = SessionContext()
    print(ctx)

    # create a session context with explicit runtime and config settings
    runtime = RuntimeEnvBuilder().with_disk_manager_os().with_fair_spill_pool(10000000)
    config = (
        SessionConfig()
        .with_create_default_catalog_and_schema(True)
        .with_default_catalog_and_schema("foo", "bar")
        .with_target_partitions(8)
        .with_information_schema(True)
        .with_repartition_joins(False)
        .with_repartition_aggregations(False)
        .with_repartition_windows(False)
        .with_parquet_pruning(False)
        .set("datafusion.execution.parquet.pushdown_filters", "true")
    )
    ctx = SessionContext(config, runtime)
    print(ctx)

Maximizing CPU Usage
--------------------

DataFusion uses partitions to parallelize work. For small queries the
default configuration (number of CPU cores) is often sufficient, but to
fully utilize available hardware you can tune how many partitions are
created and when DataFusion will repartition data automatically.

Configure a ``SessionContext`` with a higher partition count:

.. code-block:: python

    from datafusion import SessionConfig, SessionContext

    # allow up to 16 concurrent partitions
    config = SessionConfig().with_target_partitions(16)
    ctx = SessionContext(config)

Automatic repartitioning for joins, aggregations, window functions and
other operations can be enabled to increase parallelism:

.. code-block:: python

    config = (
        SessionConfig()
        .with_target_partitions(16)
        .with_repartition_joins(True)
        .with_repartition_aggregations(True)
        .with_repartition_windows(True)
    )

Manual repartitioning is available on DataFrames when you need precise
control:

.. code-block:: python

    from datafusion import col

    df = ctx.read_parquet("data.parquet")

    # Evenly divide into 16 partitions
    df = df.repartition(16)

    # Or partition by the hash of a column
    df = df.repartition_by_hash(col("a"), num=16)

    result = df.collect()


You can read more about available :py:class:`~datafusion.context.SessionConfig` options in the `rust DataFusion Configuration guide <https://arrow.apache.org/datafusion/user-guide/configs.html>`_,
and about :code:`RuntimeEnvBuilder` options in the rust `online API documentation <https://docs.rs/datafusion/latest/datafusion/execution/runtime_env/struct.RuntimeEnvBuilder.html>`_.
