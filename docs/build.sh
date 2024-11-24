#!/usr/bin/env bash
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

set -e

if [ ! -f pokemon.csv ]; then
    curl -O https://gist.githubusercontent.com/ritchie46/cac6b337ea52281aa23c049250a4ff03/raw/89a957ff3919d90e6ef2d34235e6bf22304f3366/pokemon.csv
fi

if [ ! -f yellow_tripdata_2021-01.parquet ]; then
    curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
fi

rm -rf build 2> /dev/null
rm -rf temp 2> /dev/null
mkdir temp
cp -rf source/* temp/
make SOURCEDIR=`pwd`/temp html
