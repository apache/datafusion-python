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

# Three libraries, one distributed query

A worked example of what `datafusion-python`'s extension protocol is *for*:
several independently compiled libraries, none of which knows about the
others, cooperating on a single query whose work runs in separate operating
system processes.

Everything here is real. The workers are separate interpreters. The plan they
run was serialized by the driver and decoded by them. If you break the
serialization, the tests fail.

## The three libraries

| Crate | Owns | Installed with |
| --- | --- | --- |
| `udf-library` (`dfx_udfs`) | a scalar function, an aggregate, a window function | **by hand** — `register_udf` plus two `with_*_extension_codec` calls |
| `storage-library` (`dfx_storage`) | a Parquet table provider and its own scan node | `with_extensions` |
| `engine-library` (`dfx_engine`) | a query planner, a stage node, and the driver/worker machinery | `with_extensions` |

One of them is deliberately old-fashioned. `dfx_udfs` exposes no
`__datafusion_session_components__`, so it cannot be installed as a bundle and
its caller has to do five things in the right order instead of one. That is
not a strawman: `SessionExtensionComponents` carries codec fields only, so a
library that contributes *functions* has nowhere to put them today. Mixed
setups are the normal case, and this example shows what one costs.

## Running it

```console
$ cd examples/distributed/engine-library
$ uv venv && uv pip install pytest pyarrow ../.. ../storage-library ../udf-library
$ uv run maturin develop
$ uv run pytest python/tests/_test*.py
```

Against the real TPC-H data — generate it as
[`examples/tpch`](../tpch/README.md) describes, then:

```console
$ uv run python ../run_tpch.py --partitions 4
```

## What actually happens

The engine's planner splits the plan at the partial aggregate, which is where
DataFusion has already split it for its own reasons: a `GROUP BY` becomes a
partial pass per input partition and a final pass that merges them, and the
partial passes are independent by construction.

```
SortPreservingMergeExec
  ProjectionExec
    AggregateExec: mode=FinalPartitioned          <- driver merges
      RepartitionExec: Hash([l_returnflag], 2)
        FFI_ExecutionPlan: ShuffleStageExec       <- shipped to workers
          AggregateExec: mode=Partial             <- one worker per partition
            FFI_ExecutionPlan: PartitionedParquetExec
```

The driver serializes the `ShuffleStageExec` subtree, starts one worker per
partition, and waits. Each worker rebuilds an equivalent session, decodes the
plan, runs *its* partition, and writes the result to an Arrow IPC file. The
driver then runs the whole query itself — and the stage node, finding the
files already there, streams them instead of recomputing.

One node does both halves of that exchange, which is why nothing has to
rewrite the plan in between. It also means a query run with no workers at all
still gets the right answer; it just does the work itself.

## The four things worth reading

**`engine-library/python/dfx_engine/session.py`** is the point of the whole
example. There is no way to snapshot a `SessionContext` and restore it
elsewhere, so worker parity cannot be automated — it has to be *built the same
way twice*, from data small enough to put in a message. Both the driver and
every worker call one `build_session`. Anything a query depends on that is not
in the `SessionSpec` is a bug waiting for a worker to find it.

**`storage-library/src/codec.rs`** is the repository's only codec that encodes
durable metadata. The others park the live object in a process-global map and
encode an integer token, which is fine for making Rust type identity
observable in a test and useless the moment the bytes leave the process. This
one writes the file paths, the projection, and the schema, so the same bytes
decode twice, decode on ten workers, and decode tomorrow.

**`udf-library/python/tests/_test_udfs.py`** shows that installing a
library's codec is an *alternative* to registering its functions, not an
addition. Three workers, three configurations:

| worker has | result |
| --- | --- |
| the codec, no registrations | works; the codec rebuilds each function from its name |
| the registrations, no codec | works; the registry answers first and the codec is never consulted |
| neither | fails, naming `dfx_net_revenue` |

The middle row is the trap. On the driver, where the functions are always
registered, a broken or missing codec looks completely fine.

**`engine-library/python/tests/_test_three_libraries.py`** runs the queries,
and pins the failure modes next to the successes — including a Python UDF that
works on the driver and fails on the worker.

## Things this example is not

It writes shuffle results to local files, so "distributed" means several
processes on one machine. Adding a network is a transport change and would not
alter anything above it.

It holds one partition of results in memory before writing, because an Arrow
IPC stream needs its schema up front. A production engine would stream to the
file and track completion separately.

It has one stage. A real engine chains them, and the interesting problems —
scheduling, retries, straggler handling, memory limits — all live in the part
this example replaces with `subprocess.Popen` and a `for` loop.

It is slower than running the query in one process. Four processes on one
laptop cannot beat one process that skips a round trip through Arrow IPC
files. The comparison the tests make is *agreement*, not speed.

## Further reading

- [Distributed query engines](https://datafusion.apache.org/python/user-guide/distributing-work/query-engines.html)
  — using an engine, and the checklist for what a worker has to reproduce.
- [Extension Guide](https://datafusion.apache.org/python/extension-guide/index.html)
  — writing a library like these.
- [Encode metadata, not a handle to a live object](https://datafusion.apache.org/python/extension-guide/codecs.html#encode-metadata-not-a-handle-to-a-live-object)
  — what a codec should put on the wire, and why.
