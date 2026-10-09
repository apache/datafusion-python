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

"""A toy distributed engine, as an extension library.

Two halves, because a real engine has two: the Rust side owns the query
planner, the stage node, the codec that carries it, and a config extension;
the Python side owns the session factory, the driver, and the worker entry
point.

Start with :mod:`dfx_engine.session` -- ``build_session`` is the piece the
rest of the example exists to motivate.
"""

from dfx_engine import _internal
from dfx_engine._internal import DfxEngineConfig, DfxEngineExtension
from dfx_engine.session import SessionSpec, build_session, expected_codec_ids

__all__ = [
    "DfxEngineConfig",
    "DfxEngineExtension",
    "SessionSpec",
    "_internal",
    "build_session",
    "expected_codec_ids",
]
