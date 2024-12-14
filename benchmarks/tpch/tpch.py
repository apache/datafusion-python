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

import argparse
from datafusion import SessionContext
import time


def bench(data_path, query_path):
    with open("results.csv", "w") as results:
        # register tables
        start = time.time()
        total_time_millis = 0

        # create context
        # runtime = (
        #     RuntimeEnvBuilder()
        #     .with_disk_manager_os()
        #     .with_fair_spill_pool(10000000)
        # )
        # config = (
        #     SessionConfig()
        #     .with_create_default_catalog_and_schema(True)
        #     .with_default_catalog_and_schema("datafusion", "tpch")
        #     .with_information_schema(True)
        # )
        # ctx = SessionContext(config, runtime)

        ctx = SessionContext()
        print("Configuration:\n", ctx)

        # register tables
        with open("create_tables.sql") as f:
            sql = ""
            for line in f.readlines():
                if line.startswith("--"):
                    continue
                sql = sql + line
                if sql.strip().endswith(";"):
                    sql = sql.strip().replace("$PATH", data_path)
                    ctx.sql(sql)
                    sql = ""

        end = time.time()
        time_millis = (end - start) * 1000
        total_time_millis += time_millis
        print("setup,{}".format(round(time_millis, 1)))
        results.write("setup,{}\n".format(round(time_millis, 1)))
        results.flush()

        # run queries
        for query in range(1, 23):
            with open("{}/q{}.sql".format(query_path, query)) as f:
                text = f.read()
                tmp = text.split(";")
                queries = []
                for str in tmp:
                    if len(str.strip()) > 0:
                        queries.append(str.strip())

                try:
                    start = time.time()
                    for sql in queries:
                        print(sql)
                        df = ctx.sql(sql)
                        # result_set = df.collect()
                        df.show()
                    end = time.time()
                    time_millis = (end - start) * 1000
                    total_time_millis += time_millis
                    print("q{},{}".format(query, round(time_millis, 1)))
                    results.write("q{},{}\n".format(query, round(time_millis, 1)))
                    results.flush()
                except Exception as e:
                    print("query", query, "failed", e)

        print("total,{}".format(round(total_time_millis, 1)))
        results.write("total,{}\n".format(round(total_time_millis, 1)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("data_path")
    parser.add_argument("query_path")
    args = parser.parse_args()
    bench(args.data_path, args.query_path)
