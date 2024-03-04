# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import gc
import timeit
import datafusion as df
from datafusion import (
    col,
    functions as f,
    RuntimeConfig,
    SessionConfig,
    SessionContext,
)
import pyarrow
from pyarrow import csv as pacsv


print("# groupby-datafusion.py", flush=True)

exec(open("./_helpers/helpers.py").read())


def ans_shape(batches):
    rows, cols = 0, 0
    for batch in batches:
        rows += batch.num_rows
        if cols == 0:
            cols = batch.num_columns
        else:
            assert cols == batch.num_columns
    return rows, cols


def execute(df):
    print(df.execution_plan().display_indent())
    return df.collect()


ver = df.__version__
git = ""
task = "groupby"
solution = "datafusion"
fun = ".groupby"
cache = "TRUE"
on_disk = "FALSE"

# experimental - support running with both DataFrame and SQL APIs
sql = True

data_name = os.environ["SRC_DATANAME"]
src_grp = os.path.join("data", data_name + ".csv")
print("loading dataset %s" % src_grp, flush=True)

schema = pyarrow.schema(
    [
        ("id4", pyarrow.int32()),
        ("id5", pyarrow.int32()),
        ("id6", pyarrow.int32()),
        ("v1", pyarrow.int32()),
        ("v2", pyarrow.int32()),
        ("v3", pyarrow.float64()),
    ]
)

data = pacsv.read_csv(
    src_grp,
    convert_options=pacsv.ConvertOptions(auto_dict_encode=True, column_types=schema),
)
print("dataset loaded")

# create a session context with explicit runtime and config settings
runtime = (
    RuntimeConfig().with_disk_manager_os().with_fair_spill_pool(64 * 1024 * 1024 * 1024)
)
config = (
    SessionConfig()
    .with_repartition_joins(False)
    .with_repartition_aggregations(False)
    .set("datafusion.execution.coalesce_batches", "false")
)
ctx = SessionContext(config, runtime)
print(ctx)

ctx.register_record_batches("x", [data.to_batches()])
print("registered record batches")
# cols = ctx.sql("SHOW columns from x")
# ans.show()

in_rows = data.num_rows
# print(in_rows, flush=True)

task_init = timeit.default_timer()

question = "sum v1 by id1"  # q1
gc.collect()
t_start = timeit.default_timer()
if sql:
    df = ctx.sql("SELECT id1, SUM(v1) AS v1 FROM x GROUP BY id1")
else:
    df = ctx.table("x").aggregate([f.col("id1")], [f.sum(f.col("v1")).alias("v1")])
ans = execute(df)

shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
print(f"q1: {t}")
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=in_rows,
    question=question,
    out_rows=shape[0],
    out_cols=shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk([chk]),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()

question = "sum v1 by id1:id2"  # q2
gc.collect()
t_start = timeit.default_timer()
if sql:
    df = ctx.sql("SELECT id1, id2, SUM(v1) AS v1 FROM x GROUP BY id1, id2")
else:
    df = ctx.table("x").aggregate(
        [f.col("id1"), f.col("id2")], [f.sum(f.col("v1")).alias("v1")]
    )
ans = execute(df)
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
print(f"q2: {t}")
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=in_rows,
    question=question,
    out_rows=shape[0],
    out_cols=shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk([chk]),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()

question = "sum v1 mean v3 by id3"  # q3
gc.collect()
t_start = timeit.default_timer()
if sql:
    df = ctx.sql("SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM x GROUP BY id3")
else:
    df = ctx.table("x").aggregate(
        [f.col("id3")],
        [
            f.sum(f.col("v1")).alias("v1"),
            f.avg(f.col("v3")).alias("v3"),
        ],
    )
ans = execute(df)
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
print(f"q3: {t}")
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = (
    df.aggregate([], [f.sum(col("v1")), f.sum(col("v3"))])
    .collect()[0]
    .to_pandas()
    .to_numpy()[0]
)
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=in_rows,
    question=question,
    out_rows=shape[0],
    out_cols=shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk([chk]),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()

question = "mean v1:v3 by id4"  # q4
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql(
    "SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM x GROUP BY id4"
).collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
print(f"q4: {t}")
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = (
    df.aggregate([], [f.sum(col("v1")), f.sum(col("v2")), f.sum(col("v3"))])
    .collect()[0]
    .to_pandas()
    .to_numpy()[0]
)
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=in_rows,
    question=question,
    out_rows=shape[0],
    out_cols=shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk([chk]),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()

question = "sum v1:v3 by id6"  # q5
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql(
    "SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM x GROUP BY id6"
).collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
print(f"q5: {t}")
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = (
    df.aggregate([], [f.sum(col("v1")), f.sum(col("v2")), f.sum(col("v3"))])
    .collect()[0]
    .to_pandas()
    .to_numpy()[0]
)
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=in_rows,
    question=question,
    out_rows=shape[0],
    out_cols=shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk([chk]),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()

question = "median v3 sd v3 by id4 id5"  # q6
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql(
    "SELECT id4, id5, approx_percentile_cont(v3, .5) AS median_v3, stddev(v3) AS stddev_v3 FROM x GROUP BY id4, id5"
).collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
print(f"q6: {t}")
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = (
    df.aggregate([], [f.sum(col("median_v3")), f.sum(col("stddev_v3"))])
    .collect()[0]
    .to_pandas()
    .to_numpy()[0]
)
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=in_rows,
    question=question,
    out_rows=shape[0],
    out_cols=shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk([chk]),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()

question = "max v1 - min v2 by id3"  # q7
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql(
    "SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 FROM x GROUP BY id3"
).collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
print(f"q7: {t}")
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("range_v1_v2"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=in_rows,
    question=question,
    out_rows=shape[0],
    out_cols=shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk([chk]),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()

question = "largest two v3 by id6"  # q8
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql(
    "SELECT id6, v3 from (SELECT id6, v3, row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS row FROM x) t WHERE row <= 2"
).collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
print(f"q8: {t}")
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v3"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=in_rows,
    question=question,
    out_rows=shape[0],
    out_cols=shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk([chk]),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()

question = "regression v1 v2 by id2 id4"  # q9
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT corr(v1, v2) as corr FROM x GROUP BY id2, id4").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
print(f"q9: {t}")
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("corr"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=in_rows,
    question=question,
    out_rows=shape[0],
    out_cols=shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk([chk]),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()

question = "sum v3 count by id1:id6"  # q10
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql(
    "SELECT id1, id2, id3, id4, id5, id6, SUM(v3) as v3, COUNT(*) AS cnt FROM x GROUP BY id1, id2, id3, id4, id5, id6"
).collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
print(f"q10: {t}")
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = (
    df.aggregate([], [f.sum(col("v3")), f.sum(col("cnt"))])
    .collect()[0]
    .to_pandas()
    .to_numpy()[0]
)
chkt = timeit.default_timer() - t_start
write_log(
    task=task,
    data=data_name,
    in_rows=in_rows,
    question=question,
    out_rows=shape[0],
    out_cols=shape[1],
    solution=solution,
    version=ver,
    git=git,
    fun=fun,
    run=1,
    time_sec=t,
    mem_gb=m,
    cache=cache,
    chk=make_chk([chk]),
    chk_time_sec=chkt,
    on_disk=on_disk,
)
del ans
gc.collect()

print(
    "grouping finished, took %0.fs" % (timeit.default_timer() - task_init),
    flush=True,
)

exit(0)
