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

# datafusion

::: datafusion
    options:
      members: false

## Submodules

| Module | Description |
| --- | --- |
| [`catalog`](catalog.md) | Catalog, schema, and table providers |
| [`common`](common.md) | Common types shared across the API |
| [`context`](context.md) | `SessionContext`, session config, and runtime |
| [`dataframe`](dataframe.md) | `DataFrame` query builder and write options |
| [`dataframe_formatter`](dataframe_formatter.md) | HTML/text rendering for DataFrames |
| [`expr`](expr.md) | Expression tree (`Expr`, window frames, grouping sets) |
| [`functions`](functions.md) | 290+ built-in scalar, aggregate, and window functions |
| [`input`](input.md) | Input source plugins |
| [`io`](io.md) | `read_csv`, `read_parquet`, `read_json`, `read_avro` |
| [`ipc`](ipc.md) | Arrow IPC serialization for DataFrames and expressions |
| [`object_store`](object_store.md) | Object store backends (S3, GCS, Azure, local) |
| [`options`](options.md) | Read-option configuration types |
| [`plan`](plan.md) | Logical and physical plan introspection |
| [`record_batch`](record_batch.md) | `RecordBatch` and `RecordBatchStream` |
| [`substrait`](substrait.md) | Substrait plan serialization |
| [`unparser`](unparser.md) | Convert logical plans back to SQL |
| [`user_defined`](user_defined.md) | User-defined scalar, aggregate, window, and table functions |

## Top-level names

These names live on the `datafusion` package itself and are imported as
`from datafusion import <name>`.

### Column builders

::: datafusion.col.col

::: datafusion.col.column

### Literal builders

::: datafusion.lit

::: datafusion.literal

::: datafusion.string_literal

::: datafusion.str_lit

::: datafusion.literal_with_metadata

::: datafusion.lit_with_metadata
