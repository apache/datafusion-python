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

SQL
===

DataFusion also offers a SQL API, read the full reference `here <https://arrow.apache.org/datafusion/user-guide/sql/index.html>`_

.. ipython:: python

    import datafusion
    from datafusion import col
    import pyarrow

    # create a context
    ctx = datafusion.SessionContext()

    # register a CSV
    ctx.register_csv('pokemon', 'pokemon.csv')

    # create a new statement via SQL
    df = ctx.sql('SELECT "Attack"+"Defense", "Attack"-"Defense" FROM pokemon')

    # collect and convert to pandas DataFrame
    df.to_pandas()

Automatic variable registration
-------------------------------

You can opt-in to DataFusion automatically registering Arrow-compatible Python
objects that appear in SQL queries. This removes the need to call
``register_*`` helpers explicitly when working with in-memory data structures.

.. code-block:: python

    import pyarrow as pa
    from datafusion import SessionContext

    ctx = SessionContext(auto_register_python_objects=True)

    orders = pa.Table.from_pydict({"item": ["apple", "pear"], "qty": [5, 2]})

    result = ctx.sql("SELECT item, qty FROM orders WHERE qty > 2")
    print(result.to_pandas())

The feature inspects the call stack for variables whose names match missing
tables and registers them if they expose Arrow data (including pandas and
Polars DataFrames). Existing contexts can enable or disable the behavior at
runtime through :py:meth:`SessionContext.set_python_table_lookup` or by passing
``auto_register_python_objects`` when constructing the session.
