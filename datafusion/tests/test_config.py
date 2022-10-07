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

from datafusion import Config
import pytest


@pytest.fixture
def config():
    return Config()


def test_get_then_set(config):
    config_key = "datafusion.optimizer.filter_null_join_keys"

    assert config.get(config_key).as_py() is False

    config.set(config_key, True)
    assert config.get(config_key).as_py() is True


def test_get_all(config):
    config.get_all()


def test_get_invalid_config(config):
    assert config.get("not.valid.key") is None
