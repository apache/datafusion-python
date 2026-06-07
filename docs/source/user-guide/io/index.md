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

# IO

DataFusion can read and write a range of file formats and stream data in
through Arrow-compatible Python objects.

## File formats

| Format | Reader | Notes |
|---|---|---|
| [Apache Arrow](arrow.ipynb) | [`SessionContext.read_arrow`][datafusion.context.SessionContext.read_arrow] | Single Arrow IPC file. |
| [Avro](avro.md) | [`SessionContext.read_avro`][datafusion.context.SessionContext.read_avro] | Schema-on-read; requires the Avro feature in the wheel. |
| [CSV](csv.md) | [`SessionContext.read_csv`][datafusion.context.SessionContext.read_csv] | Header inference, custom delimiters, gzip/bz2 compression. |
| [JSON](json.md) | [`SessionContext.read_json`][datafusion.context.SessionContext.read_json] | Newline-delimited JSON; one record per line. |
| [Parquet](parquet.md) | [`SessionContext.read_parquet`][datafusion.context.SessionContext.read_parquet] | Predicate / projection push-down, partitioned datasets. |

## Custom sources

- [Table Provider](table_provider.md) — register an arbitrary data source
  (Delta Lake, Iceberg, your own Rust crate, etc.) by implementing the
  table-provider FFI interface.

## See also

- [Data Sources](../data-sources.ipynb) — concept overview, including
  in-memory DataFrame creation from `pyarrow` / `pandas` / `polars` and
  object-store integration.
