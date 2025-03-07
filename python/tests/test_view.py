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


from datafusion import SessionContext, col, literal


def test_register_filtered_dataframe():
    ctx = SessionContext()

    data = {"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]}

    df = ctx.from_pydict(data, "my_table")

    df_filtered = df.filter(col("a") > literal(2))

    ctx.register_view("view1", df_filtered)

    df_view = ctx.sql("SELECT * FROM view1")

    filtered_results = df_view.collect()

    result_dicts = [batch.to_pydict() for batch in filtered_results]

    expected_results = [{"a": [3, 4, 5], "b": [30, 40, 50]}]

    assert result_dicts == expected_results

    df_results = df.collect()

    df_result_dicts = [batch.to_pydict() for batch in df_results]

    expected_df_results = [{"a": [1, 2, 3, 4, 5], "b": [10, 20, 30, 40, 50]}]

    assert df_result_dicts == expected_df_results
