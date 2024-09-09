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

import datafusion
import datafusion.functions
import datafusion.object_store
import datafusion.substrait

# EnumType introduced in 3.11. 3.10 and prior it was called EnumMeta.
try:
    from enum import EnumType
except ImportError:
    from enum import EnumMeta as EnumType


def missing_exports(internal_obj, wrapped_obj) -> None:
    # Special case enums - just make sure they exist since dir()
    # and other functions get overridden.
    if isinstance(wrapped_obj, EnumType):
        return

    for attr in dir(internal_obj):
        assert attr in dir(wrapped_obj)

        internal_attr = getattr(internal_obj, attr)
        wrapped_attr = getattr(wrapped_obj, attr)

        assert wrapped_attr is not None if internal_attr is not None else True

        if attr in ["__self__", "__class__"]:
            continue
        if isinstance(internal_attr, list):
            assert isinstance(wrapped_attr, list)
            for val in internal_attr:
                assert val in wrapped_attr
        elif hasattr(internal_attr, "__dict__"):
            missing_exports(internal_attr, wrapped_attr)


def test_datafusion_missing_exports() -> None:
    """Check for any missing pythone exports.

    This test verifies that every exposed class, attribute, and function in
    the internal (pyo3) module is also exposed in our python wrappers.
    """
    missing_exports(datafusion._internal, datafusion)
