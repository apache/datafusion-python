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
    from datafusion import DataFrame, SessionContext

    # create a context
    ctx = datafusion.SessionContext()

    # register a CSV
    ctx.register_csv("pokemon", "pokemon.csv")

    # create a new statement via SQL
    df = ctx.sql('SELECT "Attack"+"Defense", "Attack"-"Defense" FROM pokemon')

    # collect and convert to pandas DataFrame
    df.to_pandas()

Parameterized queries
---------------------

In DataFusion-Python 51.0.0 we introduced the ability to pass parameters
in a SQL query. These are similar in concept to
`prepared statements <https://datafusion.apache.org/user-guide/sql/prepared_statements.html>`_,
but allow passing named parameters into a SQL query. Consider this simple
example.

.. ipython:: python

    def show_attacks(ctx: SessionContext, threshold: int) -> None:
        ctx.sql(
            'SELECT "Name", "Attack" FROM pokemon WHERE "Attack" > $val', val=threshold
        ).show(num=5)
    show_attacks(ctx, 75)

When passing parameters like the example above we convert the Python objects
into their string representation. We also have special case handling
for :py:class:`~datafusion.dataframe.DataFrame` objects, since they cannot simply
be turned into string representations for an SQL query. In these cases we
will register a temporary view in the :py:class:`~datafusion.context.SessionContext`
using a generated table name.

The formatting for passing string replacement objects is to precede the
variable name with a single ``$``. This works for all dialects in
the SQL parser except ``hive`` and ``mysql``. Since these dialects do not
support named placeholders, we are unable to do this type of replacement.
We recommend either switching to another dialect or using Python
f-string style replacement.

.. warning::

    To support DataFrame parameterized queries, your session must support
    registration of temporary views. The default
    :py:class:`~datafusion.catalog.CatalogProvider` and
    :py:class:`~datafusion.catalog.SchemaProvider` do have this capability.
    If you have implemented custom providers, it is important that temporary
    views do not persist across :py:class:`~datafusion.context.SessionContext`
    or you may get unintended consequences.

The following example shows passing in both a :py:class:`~datafusion.dataframe.DataFrame`
object as well as a Python object to be used in parameterized replacement.

.. ipython:: python

    def show_column(
        ctx: SessionContext, column: str, df: DataFrame, threshold: int
    ) -> None:
        ctx.sql(
            'SELECT "Name", $col FROM $df WHERE $col > $val',
            col=column,
            df=df,
            val=threshold,
        ).show(num=5)
    df = ctx.table("pokemon")
    show_column(ctx, '"Defense"', df, 75)

The approach implemented for conversion of variables into a SQL query
relies on string conversion. This has the potential for data loss,
specifically for cases like floating point numbers. If you need to pass
variables into a parameterized query and it is important to maintain the
original value without conversion to a string, then you can use the
optional parameter ``param_values`` to specify these. This parameter
expects a dictionary mapping from the parameter name to a Python
object. Those objects will be cast into a
`PyArrow Scalar Value <https://arrow.apache.org/docs/python/generated/pyarrow.Scalar.html>`_.

Using ``param_values`` will rely on the SQL dialect you have configured
for your session. This can be set using the :ref:`configuration options <configuration>`
of your :py:class:`~datafusion.context.SessionContext`. Similar to how
`prepared statements <https://datafusion.apache.org/user-guide/sql/prepared_statements.html>`_
work, these parameters are limited to places where you would pass in a
scalar value, such as a comparison.

.. ipython:: python

    def param_attacks(ctx: SessionContext, threshold: int) -> None:
        ctx.sql(
            'SELECT "Name", "Attack" FROM pokemon WHERE "Attack" > $val',
            param_values={"val": threshold},
        ).show(num=5)
    param_attacks(ctx, 75)
