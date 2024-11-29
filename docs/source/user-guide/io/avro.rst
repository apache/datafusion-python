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

.. _io_avro:

Avro
====

`Avro <https://avro.apache.org/>`_ is a serialization format for record data. Reading an avro file is very straightforward
with :py:func:`~datafusion.context.SessionContext.read_avro`

.. code-block:: python


    from datafusion import SessionContext

    ctx = SessionContext()
    df = ctx.read_avro("file.avro")