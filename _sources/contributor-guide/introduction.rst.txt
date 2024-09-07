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

Introduction
============
We welcome and encourage contributions of all kinds, such as:

1. Tickets with issue reports of feature requests
2. Documentation improvements
3. Code, both PR and (especially) PR Review.

In addition to submitting new PRs, we have a healthy tradition of community members reviewing each other’s PRs.
Doing so is a great way to help the community as well as get more familiar with Rust and the relevant codebases.

How to develop
--------------

This assumes that you have rust and cargo installed. We use the workflow recommended by `pyo3 <https://github.com/PyO3/pyo3>`_ and `maturin <https://github.com/PyO3/maturin>`_.

Bootstrap:

.. code-block:: shell

    # fetch this repo
    git clone git@github.com:apache/arrow-datafusion-python.git
    # prepare development environment (used to build wheel / install in development)
    python3 -m venv venv
    # activate the venv
    source venv/bin/activate
    # update pip itself if necessary
    python -m pip install -U pip
    # install dependencies (for Python 3.8+)
    python -m pip install -r requirements-310.txt

The tests rely on test data in git submodules.

.. code-block:: shell

    git submodule init
    git submodule update


Whenever rust code changes (your changes or via `git pull`):

.. code-block:: shell

   # make sure you activate the venv using "source venv/bin/activate" first
   maturin develop
   python -m pytest

Running & Installing pre-commit hooks
-------------------------------------

arrow-datafusion-python takes advantage of `pre-commit <https://pre-commit.com/>`_ to assist developers with code linting to help reduce the number of commits that ultimately fail in CI due to linter errors. Using the pre-commit hooks is optional for the developer but certainly helpful for keeping PRs clean and concise.

Our pre-commit hooks can be installed by running :code:`pre-commit install`, which will install the configurations in your ARROW_DATAFUSION_PYTHON_ROOT/.github directory and run each time you perform a commit, failing to complete the commit if an offending lint is found allowing you to make changes locally before pushing.

The pre-commit hooks can also be run adhoc without installing them by simply running :code:`pre-commit run --all-files`

Guidelines for Separating Python and Rust Code
----------------------------------------------

Version 40 of ``datafusion-python`` introduced ``python`` wrappers around the ``pyo3`` generated code to vastly improve the user experience. (See the `blog post <https://datafusion.apache.org/blog/2024/08/20/python-datafusion-40.0.0/>`_ and `pull request <https://github.com/apache/datafusion-python/pull/750>`_ for more details.)

Mostly, the ``python`` code is limited to pure wrappers with type hints and good docstrings, but there are a few reasons for when the code does more:

1. Trivial aliases like :py:func:`~datafusion.functions.array_append` and :py:func:`~datafusion.functions.list_append`.
2. Simple type conversion, like from a ``path`` to a ``string`` of the path or from ``number`` to ``lit(number)``.
3. The additional code makes an API **much** more pythonic, like we do for :py:func:`~datafusion.functions.named_struct` (see `source code <https://github.com/apache/datafusion-python/blob/a0913c728f5f323c1eb4913e614c9d996083e274/python/datafusion/functions.py#L1040-L1046>`_).


Update Dependencies
-------------------

To change test dependencies, change the `requirements.in` and run

.. code-block:: shell

    # install pip-tools (this can be done only once), also consider running in venv
    python -m pip install pip-tools
    python -m piptools compile --generate-hashes -o requirements-310.txt


To update dependencies, run with `-U`

.. code-block:: shell

   python -m piptools compile -U --generate-hashes -o requirements-310.txt


More details about pip-tools `here <https://github.com/jazzband/pip-tools>`_
