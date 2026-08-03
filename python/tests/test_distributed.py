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

import pytest
from datafusion import SessionContext, Worker, WorkerQueryContext


def test_create_worker():
    worker = Worker()

    assert worker.with_version("local") is worker
    assert worker.with_max_message_size(1024) is worker


def test_create_worker_with_callable_session_builder():
    def build_session_state(context: WorkerQueryContext) -> SessionContext:
        return context.session_context()

    worker = Worker.from_session_builder(build_session_state)

    assert isinstance(worker, Worker)


def test_worker_session_builder_requires_callable():
    class InvalidWorkerSessionBuilder:
        build_session_state = "not-callable"

    with pytest.raises(TypeError, match="callable"):
        Worker.from_session_builder(InvalidWorkerSessionBuilder())


def test_worker_serve_rejects_invalid_bind_address():
    with pytest.raises(Exception, match="invalid worker bind address"):
        Worker().serve("not-an-address", 50051)
