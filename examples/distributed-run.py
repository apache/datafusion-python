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

import os
from argparse import ArgumentParser
from pathlib import Path

from datafusion import SessionConfig, SessionContext

DEFAULT_PARQUET_PATH = "yellow_tripdata_2021-01.parquet"
PARQUET_DOWNLOAD_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2021-01.parquet"
)


class LocalhostWorkerResolver:
    def __init__(self, ports: list[str]) -> None:
        self.ports = ports

    def get_urls(self) -> list[str]:
        return [f"http://127.0.0.1:{port}" for port in self.ports]


def worker_ports_from_env() -> list[str]:
    workers = os.environ.get("WORKERS", "")
    ports = [port.strip() for port in workers.split(",") if port.strip()]
    if not ports:
        msg = "Set WORKERS to a comma-separated list of localhost worker ports"
        raise RuntimeError(msg)
    return ports


parser = ArgumentParser()
parser.add_argument("parquet_path", nargs="?", default=DEFAULT_PARQUET_PATH)
parser.add_argument(
    "--plan",
    action="store_true",
    help="print the distributed physical plan instead of running the query",
)
args = parser.parse_args()
parquet_path = args.parquet_path
if "://" not in parquet_path:
    local_parquet_path = Path(parquet_path).expanduser()
    if not local_parquet_path.exists():
        parser.error(
            f"Parquet file {parquet_path!r} was not found. Download the example "
            f"data with:\n  curl -LO {PARQUET_DOWNLOAD_URL}\n"
            "or pass the path to an existing parquet file."
        )
    parquet_path = str(local_parquet_path)

config = SessionConfig().with_distributed(
    LocalhostWorkerResolver(worker_ports_from_env()),
)
ctx = SessionContext(config)
ctx.sql("SET distributed.file_scan_config_bytes_per_partition = 1")
ctx.register_parquet("taxi", parquet_path)
df = ctx.sql(
    "select passenger_count, count(*) from taxi where passenger_count is not null group by passenger_count order by passenger_count"
)
if args.plan:
    print(df.execution_plan().display_distributed())
else:
    df.show()
