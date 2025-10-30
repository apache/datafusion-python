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

"""
Common utilities for running TPC-H examples.
"""

from pathlib import Path


def get_data_path(filename: str) -> Path:
    path = Path(__file__).resolve().parent

    return path / "data" / filename


def get_answer_file(answer_file: str) -> Path:
    path = Path(__file__).resolve().parent

    return path / "../../benchmarks/tpch/data/answers" / f"{answer_file}.out"
