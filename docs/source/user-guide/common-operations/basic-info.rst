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

Basic Operations
================

In this section, you will learn how to display essential details of DataFrames using specific functions.

.. ipython:: python
    
    from datafusion import SessionContext
    import random
    
    ctx = SessionContext()
    df = ctx.from_pydict({
        "nrs": [1, 2, 3, 4, 5],
        "names": ["python", "ruby", "java", "haskell", "go"],
        "random": random.sample(range(1000), 5),
        "groups": ["A", "A", "B", "C", "B"],
    })
    df

Use :py:func:`~datafusion.dataframe.DataFrame.limit` to view the top rows of the frame:

.. ipython:: python

    df.limit(2)

Display the columns of the DataFrame using :py:func:`~datafusion.dataframe.DataFrame.schema`:

.. ipython:: python

    df.schema()

The method :py:func:`~datafusion.dataframe.DataFrame.to_pandas` uses pyarrow to convert to pandas DataFrame, by collecting the batches,
passing them to an Arrow table, and then converting them to a pandas DataFrame.

.. ipython:: python

    df.to_pandas()

:py:func:`~datafusion.dataframe.DataFrame.describe` shows a quick statistic summary of your data:

.. ipython:: python

    df.describe()

