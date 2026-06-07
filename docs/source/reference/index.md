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

# API Reference

The public API of `datafusion` is exported from the top-level package. Every
symbol below is importable directly: `from datafusion import SessionContext`.

| Symbol | Page |
|---|---|
| `Accumulator` | [User-Defined Functions](user_defined.md) |
| `AggregateUDF` | [User-Defined Functions](user_defined.md) |
| `Catalog` | [Catalog](catalog.md) |
| `CsvReadOptions` | [Options](options.md) |
| `DFSchema` | [Common](common.md) |
| `DataFrame` | [DataFrame](dataframe.md) |
| `DataFrameWriteOptions` | [DataFrame](dataframe.md) |
| `ExecutionPlan` | [Plan](plan.md) |
| `ExplainFormat` | [DataFrame](dataframe.md) |
| `Expr` | [Expr](expr.md) |
| `InsertOp` | [DataFrame](dataframe.md) |
| `LogicalPlan` | [Plan](plan.md) |
| `Metric` | [Plan](plan.md) |
| `MetricsSet` | [Plan](plan.md) |
| `ParquetColumnOptions` | [DataFrame](dataframe.md) |
| `ParquetWriterOptions` | [DataFrame](dataframe.md) |
| `RecordBatch` | [RecordBatch](record_batch.md) |
| `RecordBatchStream` | [RecordBatch](record_batch.md) |
| `RuntimeEnvBuilder` | [SessionContext](context.md) |
| `SQLOptions` | [SessionContext](context.md) |
| `ScalarUDF` | [User-Defined Functions](user_defined.md) |
| `SessionConfig` | [SessionContext](context.md) |
| `SessionContext` | [SessionContext](context.md) |
| `Table` | [Catalog](catalog.md) |
| `TableFunction` | [User-Defined Functions](user_defined.md) |
| `TableProviderFactory` | [Catalog](catalog.md) |
| `TableProviderFactoryExportable` | [Catalog](catalog.md) |
| `WindowFrame` | [Expr](expr.md) |
| `WindowUDF` | [User-Defined Functions](user_defined.md) |
| `col`, `column` | [Expr](expr.md) |
| `configure_formatter` | [DataFrame](dataframe.md) |
| `functions` | [Functions](functions.md) |
| `ipc` | [IPC](ipc.md) |
| `lit`, `literal` | [Expr](expr.md) |
| `object_store` | [Object Store](object_store.md) |
| `read_avro`, `read_csv`, `read_json`, `read_parquet` | [I/O](io.md) |
| `substrait` | [Substrait](substrait.md) |
| `udaf`, `udf`, `udtf`, `udwf` | [User-Defined Functions](user_defined.md) |
| `unparser` | [Unparser](unparser.md) |
