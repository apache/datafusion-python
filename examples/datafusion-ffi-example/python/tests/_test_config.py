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

from datafusion import SessionConfig, SessionContext
from datafusion_ffi_example import MyConfig


def test_config_extension_show_set():
    config = MyConfig()
    config = SessionConfig(
        {"datafusion.catalog.information_schema": "true"}
    ).with_extension(config)
    config.set("my_config.baz_count", "42")
    ctx = SessionContext(config)

    result = ctx.sql("SHOW my_config.baz_count;").collect()
    assert result[0][1][0].as_py() == "42"

    ctx.sql("SET my_config.baz_count=1;")
    result = ctx.sql("SHOW my_config.baz_count;").collect()
    assert result[0][1][0].as_py() == "1"
