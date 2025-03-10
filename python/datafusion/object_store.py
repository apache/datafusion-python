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
"""Object store functionality."""

from ._internal import object_store

AmazonS3 = object_store.AmazonS3
GoogleCloud = object_store.GoogleCloud
LocalFileSystem = object_store.LocalFileSystem
MicrosoftAzure = object_store.MicrosoftAzure
Http = object_store.Http

__all__ = ["AmazonS3", "GoogleCloud", "Http", "LocalFileSystem", "MicrosoftAzure"]
