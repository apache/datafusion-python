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
"""Miscellaneous helper utilities for DataFusion's Python bindings."""

from __future__ import annotations

from importlib import import_module, util
from typing import Any

_PYARROW_DATASET_TYPES: tuple[type[Any], ...]
_dataset_spec = util.find_spec("pyarrow.dataset")
if _dataset_spec is None:  # pragma: no cover - optional dependency at runtime
    _PYARROW_DATASET_TYPES = ()
else:  # pragma: no cover - exercised in environments with pyarrow installed
    _dataset_module = import_module("pyarrow.dataset")
    dataset_base = getattr(_dataset_module, "Dataset", None)
    dataset_types: set[type[Any]] = set()
    if isinstance(dataset_base, type):
        dataset_types.add(dataset_base)
        for value in vars(_dataset_module).values():
            if isinstance(value, type) and issubclass(value, dataset_base):
                dataset_types.add(value)
    _PYARROW_DATASET_TYPES = tuple(dataset_types)
