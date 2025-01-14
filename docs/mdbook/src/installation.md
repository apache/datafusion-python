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
# Installation

DataFusion is easy to install, just like any other Python library.

## Using uv

If you do not yet have a virtual environment, create one:

```bash
uv venv
```

You can add datafusion to your virtual environment with the usual:

```bash
uv pip install datafusion
```

Or, to add to a project:

```bash
uv add datafusion
```

## Using pip

``` bash
pip install datafusion
```

## uv & JupyterLab setup

This section explains how to install DataFusion in a uv environment with other libraries that allow for a nice Jupyter workflow.  This setup is completely optional.  These steps are only needed if you'd like to run DataFusion in a Jupyter notebook and have an interface like this:

![DataFusion in Jupyter](https://github.com/MrPowers/datafusion-book/raw/main/src/images/datafusion-jupyterlab.png)

Create a virtual environment with DataFusion, Jupyter, and other useful dependencies and start the desktop application.

```bash
uv venv
uv pip install datafusion jupyterlab jupyterlab_code_formatter
uv run jupyter lab
```

## Examples

See the [DataFusion Python Examples](https://github.com/apache/arrow-datafusion-python/tree/main/examples) for a variety of Python scripts that show DataFusion in action!
