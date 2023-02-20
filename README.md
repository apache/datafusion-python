<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# DataFusion in Python

[![Python test](https://github.com/apache/arrow-datafusion-python/actions/workflows/test.yaml/badge.svg)](https://github.com/apache/arrow-datafusion-python/actions/workflows/test.yaml)
[![Python Release Build](https://github.com/apache/arrow-datafusion-python/actions/workflows/build.yml/badge.svg)](https://github.com/apache/arrow-datafusion-python/actions/workflows/build.yml)

This is a Python library that binds to [Apache Arrow](https://arrow.apache.org/) in-memory query engine [DataFusion](https://github.com/apache/arrow-datafusion).

Like pyspark, it allows you to build a plan through SQL or a DataFrame API against in-memory data, parquet or CSV
files, run it in a multi-threaded environment, and obtain the result back in Python.

It also allows you to use UDFs and UDAFs for complex operations.

The major advantage of this library over other execution engines is that this library achieves zero-copy between
Python and its execution engine: there is no cost in using UDFs, UDAFs, and collecting the results to Python apart
from having to lock the GIL when running those operations.

Its query engine, DataFusion, is written in [Rust](https://www.rust-lang.org/), which makes strong assumptions
about thread safety and lack of memory leaks.

There is also experimental support for executing SQL against other DataFrame libraries, such as Polars, Pandas, and any 
drop-in replacements for Pandas.

Technically, zero-copy is achieved via the [c data interface](https://arrow.apache.org/docs/format/CDataInterface.html).

## Example Usage

The following example demonstrates running a SQL query against a Parquet file using DataFusion, storing the results
in a Pandas DataFrame, and then plotting a chart.

The Parquet file used in this example can be downloaded from the following page:

- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

See the [examples](examples) directory for more examples.

```python
from datafusion import SessionContext
import pandas as pd
import pyarrow as pa

# Create a DataFusion context
ctx = SessionContext()

# Register table with context
ctx.register_parquet('taxi', 'yellow_tripdata_2021-01.parquet')

# Execute SQL
df = ctx.sql("select passenger_count, count(*) "
             "from taxi "
             "where passenger_count is not null "
             "group by passenger_count "
             "order by passenger_count")

# collect as list of pyarrow.RecordBatch
results = df.collect()

# get first batch
batch = results[0]

# convert to Pandas
df = batch.to_pandas()

# create a chart
fig = df.plot(kind="bar", title="Trip Count by Number of Passengers").get_figure()
fig.savefig('chart.png')
```

This produces the following chart:

![Chart](examples/chart.png)

## Substrait Support

`arrow-datafusion-python` has bindings which allow for serializing a SQL query to substrait protobuf format and deserializing substrait protobuf bytes to a DataFusion `LogicalPlan`, `PyLogicalPlan` in a Python context, which can then be executed.

### Example of Serializing/Deserializing Substrait Plans

```python
from datafusion import SessionContext
from datafusion import substrait as ss

# Create a DataFusion context
ctx = SessionContext()

# Register table with context
ctx.register_parquet('aggregate_test_data', './testing/data/csv/aggregate_test_100.csv')

substrait_plan = ss.substrait.serde.serialize_to_plan("SELECT * FROM aggregate_test_data", ctx)
# type(substrait_plan) -> <class 'datafusion.substrait.plan'>

# Alternative serialization approaches
# type(substrait_bytes) -> <class 'list'>, at this point the bytes can be distributed to file, network, etc safely
# where they could subsequently be deserialized on the receiving end.
substrait_bytes = ss.substrait.serde.serialize_bytes("SELECT * FROM aggregate_test_data", ctx)

# Imagine here bytes would be read from network, file, etc ... for example brevity this is omitted and variable is simply reused
# type(substrait_plan) -> <class 'datafusion.substrait.plan'>
substrait_plan = ss.substrait.serde.deserialize_bytes(substrait_bytes)

# type(df_logical_plan) -> <class 'substrait.LogicalPlan'>
df_logical_plan = ss.substrait.consumer.from_substrait_plan(ctx, substrait_plan)

# Back to Substrait Plan just for demonstration purposes
# type(substrait_plan) -> <class 'datafusion.substrait.plan'>
substrait_plan = ss.substrait.producer.to_substrait_plan(df_logical_plan)

```

## How to install (from pip)

### Pip

```bash
pip install datafusion
# or
python -m pip install datafusion
```

### Conda

```bash
conda install -c conda-forge datafusion
```

You can verify the installation by running:

```python
>>> import datafusion
>>> datafusion.__version__
'0.6.0'
```

## How to develop

This assumes that you have rust and cargo installed. We use the workflow recommended by [pyo3](https://github.com/PyO3/pyo3) and [maturin](https://github.com/PyO3/maturin).

The Maturin tools used in this workflow can be installed either via Conda or Pip. Both approaches should offer the same experience. Multiple approaches are only offered to appease developer preference. Bootstrapping for both Conda and Pip are as follows.

Bootstrap (Conda):

```bash
# fetch this repo
git clone git@github.com:apache/arrow-datafusion-python.git
# create the conda environment for dev
conda env create -f ./conda/environments/datafusion-dev.yaml -n datafusion-dev
# activate the conda environment
conda activate datafusion-dev
```

Bootstrap (Pip):

```bash
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
```

The tests rely on test data in git submodules.

```bash
git submodule init
git submodule update
```

Whenever rust code changes (your changes or via `git pull`):

```bash
# make sure you activate the venv using "source venv/bin/activate" first
maturin develop
python -m pytest
```

## How to update dependencies

To change test dependencies, change the `requirements.in` and run

```bash
# install pip-tools (this can be done only once), also consider running in venv
python -m pip install pip-tools
python -m piptools compile --generate-hashes -o requirements-310.txt
```

To update dependencies, run with `-U`

```bash
python -m piptools compile -U --generate-hashes -o requirements-310.txt
```

More details [here](https://github.com/jazzband/pip-tools)
