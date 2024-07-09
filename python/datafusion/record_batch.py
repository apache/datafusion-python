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

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow
    import datafusion._internal as df_internal


class RecordBatch:
    def __init__(self, record_batch: df_internal.RecordBatch) -> None:
        self.record_batch = record_batch

    def to_pyarrow(self) -> pyarrow.RecordBatch:
        return self.record_batch.to_pyarrow()


class RecordBatchStream:
    def __init__(self, record_batch_stream: df_internal.RecordBatchStream) -> None:
        self.rbs = record_batch_stream

    def next(self) -> RecordBatch | None:
        try:
            next_batch = next(self)
        except StopIteration:
            return None

        return next_batch

    def __next__(self) -> RecordBatch | None:
        next_batch = next(self.rbs)
        return RecordBatch(next_batch) if next_batch is not None else None

    def __iter__(self) -> RecordBatchStream:
        return self
