<!---  Licensed to the Apache Software Foundation (ASF) under one
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
- [Dask SQL](https://github.com/dask-contrib/dask-sql) uses DataFusion's Python bindings for SQL parsing, query planning, and logical plan optimizations, and then transpiles the logical plan to Dask operations for execution.
- [DataFusion Ballista](https://github.com/apache/datafusion-ballista) is a distributed SQL query engine that extends DataFusion's Python bindings for distributed use cases.
- [DataFusion Ray](https://github.com/apache/datafusion-ray) is another distributed query engine that uses
