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


def test_iter_releases_reader(monkeypatch, ctx):
    batches = [
        pa.RecordBatch.from_pydict({"a": [1]}),
        pa.RecordBatch.from_pydict({"a": [2]}),
    ]

    class DummyReader:
        def __init__(self, batches):
            self._iter = iter(batches)
            self.closed = False

        def __iter__(self):
            return self

        def __next__(self):
            return next(self._iter)

        def close(self):
            self.closed = True

    dummy_reader = DummyReader(batches)

    class FakeRecordBatchReader:
        @staticmethod
        def _import_from_c_capsule(*_args, **_kwargs):
            return dummy_reader

    monkeypatch.setattr(pa, "RecordBatchReader", FakeRecordBatchReader)

    df = ctx.from_pydict({"a": [1, 2]})

    for _ in df:
        break

    assert dummy_reader.closed
