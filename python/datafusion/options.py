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
"""Option conversion functions."""

from typing import Optional, Dict
from datafusion.expr import sort_list_to_raw_sort_list


def write_options_to_raw_write_options(write_options: Optional[Dict]) -> Dict:
    """Convert a dictionary of write options into the format expected by the pyo3 bindings.
    Validates that no superfluous keys are specified, then:
    - adds default keys for each expected write option.
    - converts "sort_by" into a raw sort list.
    """
    defaults = {
        "insert_operation": None,
        "single_file_output": None,
        "partition_by": None,
        "sort_by": None,
    }

    if write_options is not None:
        invalid_write_options = set(write_options) - set(defaults)
        if invalid_write_options:
            raise ValueError(f"Invalid write options: {invalid_write_options}")

        results = {**defaults, **write_options}
        if "sort_by" in write_options:
            results["sort_by"] = sort_list_to_raw_sort_list(write_options["sort_by"])

        return results
    else:
        return defaults
