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
"""Object store functionality.

The store classes in this module are provided by `pyo3-object-store`_ and give
Python-level builders for every backend supported by the Rust ``object_store``
crate.  Pass any of these to
:py:meth:`~datafusion.context.SessionContext.register_object_store` to make a
remote prefix available to DataFusion queries.

.. _pyo3-object-store: https://github.com/developmentseed/obstore
"""

from ._internal import object_store as _os

# Primary API — names match pyo3-object-store conventions.
S3Store = _os.S3Store
GCSStore = _os.GCSStore
AzureStore = _os.AzureStore
HTTPStore = _os.HTTPStore
LocalStore = _os.LocalStore
MemoryStore = _os.MemoryStore

# Convenience factory that constructs the right store from a URL string.
from_url = _os.from_url

# Backward-compatible aliases for the names used in older datafusion-python
# releases.  New code should prefer the names above.
AmazonS3 = S3Store
GoogleCloud = GCSStore
MicrosoftAzure = AzureStore
Http = HTTPStore
LocalFileSystem = LocalStore

__all__ = [
    "AmazonS3",
    "AzureStore",
    "GCSStore",
    "GoogleCloud",
    "HTTPStore",
    "Http",
    "LocalFileSystem",
    "LocalStore",
    "MemoryStore",
    "MicrosoftAzure",
    "S3Store",
    "from_url",
]
