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

======================
Registering Views
======================

You can use the ``into_view`` method to convert a DataFrame into a view and register it with the context.

.. code-block:: python

    from datafusion import SessionContext, col, literal

    # Create a DataFusion context
    ctx = SessionContext()

    # Create sample data
    data = {"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]}

    # Create a DataFrame from the dictionary
    df = ctx.from_pydict(data, "my_table")

    # Filter the DataFrame (for example, keep rows where a > 2)
    df_filtered = df.filter(col("a") > literal(2))

    # Convert the filtered DataFrame into a view
    view = df_filtered.into_view()

    # Register the view with the context
    ctx.register_table("view1", view)

    # Now run a SQL query against the registered view
    df_view = ctx.sql("SELECT * FROM view1")

    # Collect the results
    results = df_view.collect()

    # Convert results to a list of dictionaries for display
    result_dicts = [batch.to_pydict() for batch in results]

    print(result_dicts)

This will output:

.. code-block:: python

    [{'a': [3, 4, 5], 'b': [30, 40, 50]}]
