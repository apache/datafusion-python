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

(distributing_work)=

# Distributing work

A single {py:class}`~datafusion.SessionContext` already uses every
core on the machine — DataFusion partitions and parallelizes within
a process without being asked. See
{doc}`../configuration` for tuning that.

This section is about the step after that: getting work onto more
than one process or more than one machine. There are two roads, and
they suit different problems.

## Pick your road

**You decide the partitioning → ship expressions.**

You already know how the data divides — one file per worker, one
customer per worker, one parameter setting per worker. You build an
{py:class}`~datafusion.Expr` in the driver and hand a copy to each
worker along with its slice. Standard Python `pickle` moves it, so
{py:mod}`multiprocessing`, Ray, and anything else that ships function
arguments works with no extra machinery.

Best for embarrassingly-parallel work: parameter sweeps, per-file
transforms, scoring batches. Available today.

→ {ref}`distributed_expressions`

**A library decides the partitioning → install a query engine.**

You write one ordinary SQL or DataFrame query against a table too
large for one machine, and an engine library splits the plan into
stages, runs them on its workers, and reassembles the result. You do
not partition anything and your queries do not change.

Best for large-scale analytical queries — joins and aggregations over
data that does not fit on one node. Being built upstream; not yet
usable from datafusion-python.

→ {ref}`distributed_query_engines`

## Choosing between them

The two are not competing implementations of one feature. The
question is who owns the partitioning decision.

If you can state the partitioning in one line — "one worker per input
file" — the expression road is simpler, has no cluster to operate,
and works now. If stating it requires knowing how a join will
shuffle, that decision belongs to a query planner, which is what an
engine library provides.

They also compose. An engine handles the query; expressions handle
whatever you want to fan out around it.

```{toctree}
:maxdepth: 2

expressions
query-engines
```
