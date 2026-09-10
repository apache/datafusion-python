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

(configuration)=

# Configuration

Let's look at how we can configure DataFusion. When creating a {py:class}`~datafusion.context.SessionContext`, you can pass in
a {py:class}`~datafusion.context.SessionConfig` and {py:class}`~datafusion.context.RuntimeEnvBuilder` object. These two cover a wide range of options.

```python
from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext

# create a session context with default settings
ctx = SessionContext()
print(ctx)

# create a session context with explicit runtime and config settings
runtime = RuntimeEnvBuilder().with_disk_manager_os().with_fair_spill_pool(10000000)
config = (
    SessionConfig()
    .with_create_default_catalog_and_schema(True)
    .with_default_catalog_and_schema("foo", "bar")
    .with_target_partitions(8)
    .with_information_schema(True)
    .with_repartition_joins(False)
    .with_repartition_aggregations(False)
    .with_repartition_windows(False)
    .with_parquet_pruning(False)
    .set("datafusion.execution.parquet.pushdown_filters", "true")
)
ctx = SessionContext(config, runtime)
print(ctx)
```

## Setting options by key

The `with_*` methods cover the common options, but any option DataFusion declares can be
set by its fully qualified key with {py:meth}`~datafusion.SessionConfig.set`. The value is
always a string, and is parsed according to the type the option declares, so an unknown key
or an unparsable value raises rather than being silently ignored:

```python
config = SessionConfig().set("datafusion.execution.batch_size", "1024")
```

Every method above modifies the config in place and returns it, which is what makes the
chained style work — the object you started with is the object you end up passing to
`SessionContext`.

A whole dictionary of options can be applied at once by passing it to the
{py:class}`~datafusion.SessionConfig` constructor, which is the shape a replayed set of
settings usually arrives in:

```python
config = SessionConfig({"datafusion.execution.batch_size": "1024"})
```

Both routes reject the same keys, so which one you use does not change what is accepted. The
constructor applies its entries in an unspecified order, so a dictionary with more than one
bad key does not report a predictable one first.

One trap is worth knowing about if you read settings back out of a session and replay them
somewhere else, such as onto a worker process or into a test fixture. With
`with_information_schema(True)`, the `information_schema.df_settings` table lists the
`datafusion.runtime.*` keys alongside the rest, but those come from the runtime environment
rather than from `ConfigOptions` and cannot be set this way. Feeding that table's rows back
in verbatim will fail on the first such row, whichever route you use. Configure the runtime
through `RuntimeEnvBuilder` instead, and skip the `datafusion.runtime.` prefix when
replaying.

## Maximizing CPU Usage

DataFusion uses partitions to parallelize work. For small queries the
default configuration (number of CPU cores) is often sufficient, but to
fully utilize available hardware you can tune how many partitions are
created and when DataFusion will repartition data automatically.

Configure a `SessionContext` with a higher partition count:

```python
from datafusion import SessionConfig, SessionContext

# allow up to 16 concurrent partitions
config = SessionConfig().with_target_partitions(16)
ctx = SessionContext(config)
```

Automatic repartitioning for joins, aggregations, window functions and
other operations can be enabled to increase parallelism:

```python
config = (
    SessionConfig()
    .with_target_partitions(16)
    .with_repartition_joins(True)
    .with_repartition_aggregations(True)
    .with_repartition_windows(True)
)
```

Manual repartitioning is available on DataFrames when you need precise
control:

```python
from datafusion import col

df = ctx.read_parquet("data.parquet")

# Evenly divide into 16 partitions
df = df.repartition(16)

# Or partition by the hash of a column
df = df.repartition_by_hash(col("a"), num=16)

result = df.collect()
```

(checking_partitioning)=

### Checking what the plan actually does

`repartition` and `repartition_by_hash` are requests, not instructions. The optimizer is
free to drop a repartition nothing downstream needs, to collapse partitions again for an
operator that requires a single stream, or to substitute a repartition of its own sized by
`target_partitions`. So the number you passed is not necessarily the number you get.

{py:attr}`~datafusion.ExecutionPlan.output_partitioning` reports what the built plan does,
as opposed to what was asked of it:

```python
from datafusion import SessionConfig, SessionContext, col, functions as f

config = SessionConfig().with_target_partitions(16)
ctx = SessionContext(config)

df = ctx.read_parquet("data.parquet").repartition_by_hash(col("a"), num=8)
plan = df.aggregate([col("a")], [f.sum(col("b"))]).execution_plan()

partitioning = plan.output_partitioning
print(partitioning.scheme)            # 'Hash'
print(partitioning.partition_count)   # 16 -- target_partitions, not the 8 requested
print(partitioning.hash_expressions)  # ['a@0']
```

The request for eight partitions did not survive: the optimizer inserted its own hash
repartition at `target_partitions` instead. Had the aggregation been left off, the
repartition would have been removed altogether and the plan would report
`UnknownPartitioning` over the source's own partition count.

`UnknownPartitioning` means the plan knows how many partitions it has but nothing about how
rows are distributed across them, which is the ordinary case for a file scan.
{py:attr}`~datafusion.ExecutionPlan.partition_count` gives the same count on its own when
the scheme does not matter.

### Benchmark Example

The repository includes a benchmark script that demonstrates how to maximize CPU usage
with DataFusion. The {code}`benchmarks/max_cpu_usage.py` script shows a practical example
of configuring DataFusion for optimal parallelism.

You can run the benchmark script to see the impact of different configuration settings:

```bash
# Run with default settings (uses all CPU cores)
python benchmarks/max_cpu_usage.py

# Run with specific number of rows and partitions
python benchmarks/max_cpu_usage.py --rows 5000000 --partitions 16

# See all available options
python benchmarks/max_cpu_usage.py --help
```

Here's an example showing the performance difference between single and multiple partitions:

```bash
# Single partition - slower processing
$ python benchmarks/max_cpu_usage.py --rows=10000000 --partitions 1
Processed 10000000 rows using 1 partitions in 0.107s

# Multiple partitions - faster processing
$ python benchmarks/max_cpu_usage.py --rows=10000000 --partitions 10
Processed 10000000 rows using 10 partitions in 0.038s
```

This example demonstrates nearly 3x performance improvement (0.107s vs 0.038s) when using
10 partitions instead of 1, showcasing how proper partitioning can significantly improve
CPU utilization and query performance.

The script demonstrates several key optimization techniques:

1. **Higher target partition count**: Uses {code}`with_target_partitions()` to set the number of concurrent partitions
2. **Automatic repartitioning**: Enables repartitioning for joins, aggregations, and window functions
3. **Manual repartitioning**: Uses {code}`repartition()` to ensure all partitions are utilized
4. **CPU-intensive operations**: Performs aggregations that can benefit from parallelization

The benchmark creates synthetic data and measures the time taken to perform a sum aggregation
across the specified number of partitions. This helps you understand how partition configuration
affects performance on your specific hardware.

#### Important Considerations

The provided benchmark script demonstrates partitioning concepts using synthetic in-memory data
and simple aggregation operations. While useful for understanding basic configuration principles,
actual performance in production environments may vary significantly based on numerous factors:

**Data Sources and I/O Characteristics:**

- **Table providers**: Performance differs greatly between Parquet files, CSV files, databases, and cloud storage
- **Storage type**: Local SSD, network-attached storage, and cloud storage have vastly different characteristics
- **Network latency**: Remote data sources introduce additional latency considerations
- **File sizes and distribution**: Large files may benefit differently from partitioning than many small files

**Query and Workload Characteristics:**

- **Operation complexity**: Simple aggregations versus complex joins, window functions, or nested queries
- **Data distribution**: Skewed data may not partition evenly, affecting parallel efficiency
- **Memory usage**: Large datasets may require different memory management strategies
- **Concurrent workloads**: Multiple queries running simultaneously affect resource allocation

**Hardware and Environment Factors:**

- **CPU architecture**: Different processors have varying parallel processing capabilities
- **Available memory**: Limited RAM may require different optimization strategies
- **System load**: Other applications competing for resources affect DataFusion performance

**Recommendations for Production Use:**

To optimize DataFusion for your specific use case, it is strongly recommended to:

1. **Create custom benchmarks** using your actual data sources, formats, and query patterns
2. **Test with representative data volumes** that match your production workloads
3. **Measure end-to-end performance** including data loading, processing, and result handling
4. **Evaluate different configuration combinations** for your specific hardware and workload
5. **Monitor resource utilization** (CPU, memory, I/O) to identify bottlenecks in your environment

This approach will provide more accurate insights into how DataFusion configuration options
will impact your particular applications and infrastructure.

For more information about available {py:class}`~datafusion.context.SessionConfig` options, see the [rust DataFusion Configuration guide](https://arrow.apache.org/datafusion/user-guide/configs.html),
and about {code}`RuntimeEnvBuilder` options in the rust [online API documentation](https://docs.rs/datafusion/latest/datafusion/execution/runtime_env/struct.RuntimeEnvBuilder.html).
