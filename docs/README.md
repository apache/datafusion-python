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

# DataFusion Documentation

This folder contains the source content of the [Python API](./source/api).
This is published to https://datafusion.apache.org/python by a GitHub action
when changes are merged to the main branch.

## Dependencies

It's recommended to install build dependencies and build the documentation
inside a Python `venv` using `uv`.

To prepare building the documentation run the following on the root level of the project:

```bash
# Set up a virtual environment with the documentation dependencies
uv sync --dev --group docs --no-install-package datafusion
```

## Build & Preview

Run the provided script to build the HTML pages.

```bash
# Build the repository
uv run --no-project maturin develop --uv
# Build the documentation
uv run --no-project docs/build.sh
```

The HTML will be generated into a `build` directory in `docs`.

Preview the site on Linux by running this command.

```bash
firefox docs/build/html/index.html
```

## Release Process

This documentation is hosted at https://datafusion.apache.org/python

When the PR is merged to the `main` branch of the DataFusion
repository, a [github workflow](https://github.com/apache/datafusion-python/blob/main/.github/workflows/docs.yaml) which:

1. Builds the html content
2. Pushes the html content to the [`asf-site`](https://github.com/apache/datafusion-python/tree/asf-site) branch in this repository.

The Apache Software Foundation provides https://arrow.apache.org/,
which serves content based on the configuration in
[.asf.yaml](https://github.com/apache/datafusion-python/blob/main/.asf.yaml),
which specifies the target as https://datafusion.apache.org/python.