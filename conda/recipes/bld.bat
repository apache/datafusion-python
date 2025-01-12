#
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
#

maturin build -vv -j %CPU_COUNT% --release --strip --features substrait --manylinux off --interpreter=%PYTHON%

FOR /F "delims=" %%i IN ('dir /s /b target\wheels\*.whl') DO set datafusion_wheel=%%i

%PYTHON% -m pip install --no-deps %datafusion_wheel% -vv

cargo-bundle-licenses --format yaml --output THIRDPARTY.yml
