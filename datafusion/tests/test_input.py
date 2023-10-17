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
from datafusion.input.location import LocationInputPlugin


def test_location_input():
    location_input = LocationInputPlugin()

    cwd = os.getcwd()
    input_file = (
        cwd + "/testing/data/parquet/generated_simple_numerics/blogs.parquet"
    )
    table_name = "blog"
    tbl = location_input.build_table(input_file, table_name)
    assert "blog" == tbl.name
    assert 3 == len(tbl.columns)
    assert "blogs.parquet" in tbl.filepaths[0]
