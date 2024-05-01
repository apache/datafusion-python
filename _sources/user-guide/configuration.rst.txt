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
========

Let's look at how we can configure DataFusion. When creating a :code:`SessionContext`, you can pass in
a :code:`SessionConfig` and :code:`RuntimeConfig` object. These two cover a wide range of options.

.. code-block:: python

    from datafusion import RuntimeConfig, SessionConfig, SessionContext

    # create a session context with default settings
    ctx = SessionContext()
    print(ctx)

    # create a session context with explicit runtime and config settings
    runtime = RuntimeConfig().with_disk_manager_os().with_fair_spill_pool(10000000)
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


You can read more about available :code:`SessionConfig` options `here <https://arrow.apache.org/datafusion/user-guide/configs.html>`_,
and about :code:`RuntimeConfig` options `here https://docs.rs/datafusion/latest/datafusion/execution/runtime_env/struct.RuntimeConfig.html`_.
