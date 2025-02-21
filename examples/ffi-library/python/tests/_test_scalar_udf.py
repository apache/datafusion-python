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

import pyarrow as pa
from datafusion import SessionContext, col, ffi_udf
from datafusion_ffi_library import IsEvenFunction


def test_table_loading():
    ctx = SessionContext()
    df = ctx.from_pydict({"a": [-3, -2, None, 0, 1, 2]})

    is_even = ffi_udf(IsEvenFunction())

    result = df.select(is_even(col("a"))).collect()
    df.with_column("is_even", is_even(col("a"))).show()
    print(result)

    assert len(result) == 1
    assert result[0].num_columns == 1

    result = [r.column(0) for r in result]
    expected = [
        pa.array([False, True, None, None, False, True], type=pa.bool_()),
    ]

    assert result == expected
