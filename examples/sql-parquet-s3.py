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
import datafusion
from datafusion.object_store import AmazonS3

region = "us-east-1"
bucket_name = "yellow-trips"

s3 = AmazonS3(
    bucket_name=bucket_name,
    region=region,
    access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

ctx = datafusion.SessionContext()
path = f"s3://{bucket_name}/"
ctx.register_object_store("s3://", s3, None)

ctx.register_parquet("trips", path)

df = ctx.sql("select count(passenger_count) from trips")
df.show()
