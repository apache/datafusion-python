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

# User Guide

The user guide walks through installing DataFusion in Python, building queries
with the DataFrame API or SQL, reading and writing data, and tuning execution.

## Contents

- [Introduction](introduction.md) — what DataFusion in Python is and
  when to reach for it.
- [Concepts](concepts.md) — `SessionContext`, `DataFrame`, and
  `Expr` at a glance.
- [Data Sources](data-sources.md) — reading Parquet / CSV / JSON /
  Avro, in-memory DataFrames, object stores, Delta Lake, Iceberg,
  custom table providers, and catalogs.
- [DataFrame](dataframe/index.md) — building queries with the DataFrame
  API, rendering, and execution metrics.
- [Common Operations](common-operations/index.md) — select, filter,
  joins, aggregations, windows, expressions, UDFs/UDAFs.
- [I/O](io/index.md) — per-format reading and writing details.
- [Configuration](configuration.md) — `SessionConfig` /
  `RuntimeEnvBuilder` tuning options.
- [Distributing Work](distributing-work.md) — shipping expressions to
  worker processes via pickle / cloudpickle, FFI-capsule UDFs, and
  the sender/worker context model.
- [SQL](sql.md) — registering tables and running SQL queries.
- [Upgrade Guides](upgrade-guides.md) — notes on cross-version
  migrations.
- [AI Coding Assistants](ai-coding-assistants.md) — agent-facing
  reference material and skill files.
