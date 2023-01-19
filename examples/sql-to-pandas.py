# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datafusion import SessionContext


# Create a DataFusion context
ctx = SessionContext()

# Register table with context
ctx.register_parquet("taxi", "yellow_tripdata_2021-01.parquet")

# Execute SQL
df = ctx.sql(
    "select passenger_count, count(*) "
    "from taxi "
    "where passenger_count is not null "
    "group by passenger_count "
    "order by passenger_count"
)

# collect as list of pyarrow.RecordBatch
results = df.collect()

# get first batch
batch = results[0]

# convert to Pandas
df = batch.to_pandas()

# create a chart
fig = df.plot(
    kind="bar", title="Trip Count by Number of Passengers"
).get_figure()
fig.savefig("chart.png")
