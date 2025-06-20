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
import pytest

# EnumType introduced in 3.11. 3.10 and prior it was called EnumMeta.
try:
    from enum import EnumType
except ImportError:
    from enum import EnumMeta as EnumType


def missing_exports(internal_obj, wrapped_obj) -> None:  # noqa: C901
    """
    Identify if any of the rust exposted structs or functions do not have wrappers.

    Special handling for:
    - Raw* classes: Internal implementation details that shouldn't be exposed
    - _global_ctx: Internal implementation detail
    - __self__, __class__, __repr__: Python special attributes
    """
    # Special case enums - EnumType overrides a some of the internal functions,
    # so check all of the values exist and move on
    if isinstance(wrapped_obj, EnumType):
        expected_values = [v for v in dir(internal_obj) if not v.startswith("__")]
        for value in expected_values:
            assert value in dir(wrapped_obj)
        return

    if "__repr__" in internal_obj.__dict__ and "__repr__" not in wrapped_obj.__dict__:
        pytest.fail(f"Missing __repr__: {internal_obj.__name__}")

    for internal_attr_name in dir(internal_obj):
        wrapped_attr_name = internal_attr_name.removeprefix("Raw")
        assert wrapped_attr_name in dir(wrapped_obj)

        internal_attr = getattr(internal_obj, internal_attr_name)
        wrapped_attr = getattr(wrapped_obj, wrapped_attr_name)

        # There are some auto generated attributes that can be None, such as
        # __kwdefaults__ and __doc__. As long as these are None on the internal
        # object, it's okay to skip them. However if they do exist on the internal
        # object they must also exist on the wrapped object.
        if internal_attr is not None and wrapped_attr is None:
            pytest.fail(f"Missing attribute: {internal_attr_name}")

        if internal_attr_name in ["__self__", "__class__"]:
            continue

        if isinstance(internal_attr, list):
            assert isinstance(wrapped_attr, list)

            # We have cases like __all__ that are a list and we want to be certain that
            # every value in the list in the internal object is also in the wrapper list
            for val in internal_attr:
                if isinstance(val, str) and val.startswith("Raw"):
                    assert val[3:] in wrapped_attr
                else:
                    assert val in wrapped_attr
        elif hasattr(internal_attr, "__dict__"):
            # Check all submodules recursively
            missing_exports(internal_attr, wrapped_attr)


def test_datafusion_missing_exports() -> None:
    """Check for any missing python exports.

    This test verifies that every exposed class, attribute,
    and function in the internal (pyo3) module - datafusion._internal
    is also exposed in our python wrappers - datafusion -
    i.e., the ones exposed to the public.
    """
    missing_exports(datafusion._internal, datafusion)
