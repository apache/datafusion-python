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

.. _guide:

Introduction
============

Welcome to the User Guide for the Python bindings of Arrow DataFusion. This guide aims to provide an introduction to
DataFusion through various examples and highlight the most effective ways of using it.

Installation
------------

DataFusion is a Python library and, as such, can be installed via pip from `PyPI <https://pypi.org/project/datafusion>`__.

.. code-block:: shell

    pip install datafusion

You can verify the installation by running:

.. ipython:: python

    import datafusion
    datafusion.__version__



