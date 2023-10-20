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

## Using pip

``` bash
pip install datafusion
```

## Conda & JupyterLab setup

This section explains how to install DataFusion in a conda environment with other libraries that allow for a nice Jupyter workflow.  This setup is completely optional.  These steps are only needed if you'd like to run DataFusion in a Jupyter notebook and have an interface like this:

![DataFusion in Jupyter](https://github.com/MrPowers/datafusion-book/raw/main/src/images/datafusion-jupyterlab.png)

Create a conda environment with DataFusion, Jupyter, and other useful dependencies in the `datafusion-env.yml` file:

```
name: datafusion-env
channels:
  - conda-forge
  - defaults
dependencies:
  - python=3.9
  - ipykernel
  - nb_conda
  - jupyterlab
  - jupyterlab_code_formatter
  - isort
  - black
  - pip
  - pip:
    - datafusion

```

Create the environment with `conda env create -f datafusion-env.yml`.

Activate the environment with `conda activate datafusion-env`.

Run `jupyter lab` or open the [JupyterLab Desktop application](https://github.com/jupyterlab/jupyterlab-desktop) to start running DataFusion in a Jupyter notebook.

## Examples

See the [DataFusion Python Examples](https://github.com/apache/arrow-datafusion-python/tree/main/examples) for a variety of Python scripts that show DataFusion in action!
