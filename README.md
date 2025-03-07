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

[![Python test](https://github.com/apache/datafusion-python/actions/workflows/test.yaml/badge.svg)](https://github.com/apache/datafusion-python/actions/workflows/test.yaml)
[![Python Release Build](https://github.com/apache/datafusion-python/actions/workflows/build.yml/badge.svg)](https://github.com/apache/datafusion-python/actions/workflows/build.yml)

This is a Python library that binds to [Apache Arrow](https://arrow.apache.org/) in-memory query engine [DataFusion](https://github.com/apache/datafusion).

DataFusion's Python bindings can be used as a foundation for building new data systems in Python. Here are some examples:

- [Dask SQL](https://github.com/dask-contrib/dask-sql) uses DataFusion's Python bindings for SQL parsing, query
  planning, and logical plan optimizations, and then transpiles the logical plan to Dask operations for execution.
- [DataFusion Ballista](https://github.com/apache/datafusion-ballista) is a distributed SQL query engine that extends
  DataFusion's Python bindings for distributed use cases.
- [DataFusion Ray](https://github.com/apache/datafusion-ray) is another distributed query engine that uses
  DataFusion's Python bindings.

## Features

- Execute queries using SQL or DataFrames against CSV, Parquet, and JSON data sources.
- Queries are optimized using DataFusion's query optimizer.
- Execute user-defined Python code from SQL.
- Exchange data with Pandas and other DataFrame libraries that support PyArrow.
- Serialize and deserialize query plans in Substrait format.
- Experimental support for transpiling SQL queries to DataFrame calls with Polars, Pandas, and cuDF.

## Example Usage

The following example demonstrates running a SQL query against a Parquet file using DataFusion, storing the results
in a Pandas DataFrame, and then plotting a chart.

The Parquet file used in this example can be downloaded from the following page:

- https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

```python
from datafusion import SessionContext

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

# convert to Pandas
pandas_df = df.to_pandas()

# create a chart
fig = pandas_df.plot(kind="bar", title="Trip Count by Number of Passengers").get_figure()
fig.savefig('chart.png')
```

This produces the following chart:

![Chart](examples/chart.png)

## Registering a DataFrame as a View

You can use SessionContext's `register_view` method to convert a DataFrame into a view and register it with the context.

```python
from datafusion import SessionContext, col, literal

# Create a DataFusion context
ctx = SessionContext()

# Create sample data
data = {"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]}

# Create a DataFrame from the dictionary
df = ctx.from_pydict(data, "my_table")

# Filter the DataFrame (for example, keep rows where a > 2)
df_filtered = df.filter(col("a") > literal(2))

# Register the dataframe as a view with the context
ctx.register_view("view1", df_filtered)

# Now run a SQL query against the registered view
df_view = ctx.sql("SELECT * FROM view1")

# Collect the results
results = df_view.collect()

# Convert results to a list of dictionaries for display
result_dicts = [batch.to_pydict() for batch in results]

print(result_dicts)
```

This will output:

```python
[{'a': [3, 4, 5], 'b': [30, 40, 50]}]
```

## Configuration

It is possible to configure runtime (memory and disk settings) and configuration settings when creating a context.

```python
runtime = (
    RuntimeEnvBuilder()
    .with_disk_manager_os()
    .with_fair_spill_pool(10000000)
)
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
```

Refer to the [API documentation](https://arrow.apache.org/datafusion-python/#api-reference) for more information.

Printing the context will show the current configuration settings.

```python
print(ctx)
```

## Extensions

For information about how to extend DataFusion Python, please see the extensions page of the
[online documentation](https://datafusion.apache.org/python/).

## More Examples

See [examples](examples/README.md) for more information.

### Executing Queries with DataFusion

- [Query a Parquet file using SQL](https://github.com/apache/datafusion-python/blob/main/examples/sql-parquet.py)
- [Query a Parquet file using the DataFrame API](https://github.com/apache/datafusion-python/blob/main/examples/dataframe-parquet.py)
- [Run a SQL query and store the results in a Pandas DataFrame](https://github.com/apache/datafusion-python/blob/main/examples/sql-to-pandas.py)
- [Run a SQL query with a Python user-defined function (UDF)](https://github.com/apache/datafusion-python/blob/main/examples/sql-using-python-udf.py)
- [Run a SQL query with a Python user-defined aggregation function (UDAF)](https://github.com/apache/datafusion-python/blob/main/examples/sql-using-python-udaf.py)
- [Query PyArrow Data](https://github.com/apache/datafusion-python/blob/main/examples/query-pyarrow-data.py)
- [Create dataframe](https://github.com/apache/datafusion-python/blob/main/examples/import.py)
- [Export dataframe](https://github.com/apache/datafusion-python/blob/main/examples/export.py)

### Running User-Defined Python Code

- [Register a Python UDF with DataFusion](https://github.com/apache/datafusion-python/blob/main/examples/python-udf.py)
- [Register a Python UDAF with DataFusion](https://github.com/apache/datafusion-python/blob/main/examples/python-udaf.py)

### Substrait Support

- [Serialize query plans using Substrait](https://github.com/apache/datafusion-python/blob/main/examples/substrait.py)

## How to install

### uv

```bash
uv add datafusion
```

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

This assumes that you have rust and cargo installed. We use the workflow recommended by [pyo3](https://github.com/PyO3/pyo3) and [maturin](https://github.com/PyO3/maturin). The Maturin tools used in this workflow can be installed either via `uv` or `pip`. Both approaches should offer the same experience. It is recommended to use `uv` since it has significant performance improvements
over `pip`.

Bootstrap (`uv`):

By default `uv` will attempt to build the datafusion python package. For our development we prefer to build manually. This means
that when creating your virtual environment using `uv sync` you need to pass in the additional `--no-install-package datafusion`
and for `uv run` commands the additional parameter `--no-project`

```bash
# fetch this repo
git clone git@github.com:apache/datafusion-python.git
# create the virtual enviornment
uv sync --dev --no-install-package datafusion
# activate the environment
source .venv/bin/activate
```

Bootstrap (`pip`):

```bash
# fetch this repo
git clone git@github.com:apache/datafusion-python.git
# prepare development environment (used to build wheel / install in development)
python3 -m venv .venv
# activate the venv
source .venv/bin/activate
# update pip itself if necessary
python -m pip install -U pip
# install dependencies
python -m pip install -r pyproject.toml
```

The tests rely on test data in git submodules.

```bash
git submodule update --init
```

Whenever rust code changes (your changes or via `git pull`):

```bash
# make sure you activate the venv using "source venv/bin/activate" first
maturin develop --uv
python -m pytest
```

Alternatively if you are using `uv` you can do the following without
needing to activate the virtual environment:

```bash
uv run --no-project maturin develop --uv
uv --no-project pytest .
```

### Running & Installing pre-commit hooks

`datafusion-python` takes advantage of [pre-commit](https://pre-commit.com/) to assist developers with code linting to help reduce
the number of commits that ultimately fail in CI due to linter errors. Using the pre-commit hooks is optional for the
developer but certainly helpful for keeping PRs clean and concise.

Our pre-commit hooks can be installed by running `pre-commit install`, which will install the configurations in
your DATAFUSION_PYTHON_ROOT/.github directory and run each time you perform a commit, failing to complete
the commit if an offending lint is found allowing you to make changes locally before pushing.

The pre-commit hooks can also be run adhoc without installing them by simply running `pre-commit run --all-files`

## Running linters without using pre-commit

There are scripts in `ci/scripts` for running Rust and Python linters.

```shell
./ci/scripts/python_lint.sh
./ci/scripts/rust_clippy.sh
./ci/scripts/rust_fmt.sh
./ci/scripts/rust_toml_fmt.sh
```

## How to update dependencies

To change test dependencies, change the `pyproject.toml` and run

```bash
uv sync --dev --no-install-package datafusion
```
