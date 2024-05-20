#!/bin/bash
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

mkdir -p data/answers 2>/dev/null

set -e

# If RUN_IN_CI is set, then do not produce verbose output or use an interactive terminal
if [[ -z "${RUN_IN_CI}" ]]; then
  TERMINAL_FLAG="-it"
  VERBOSE_OUTPUT="-vf"
else
  TERMINAL_FLAG=""
  VERBOSE_OUTPUT="-f"
fi

#pushd ..
#. ./dev/build-set-env.sh
#popd

# Generate data into the ./data directory if it does not already exist
FILE=./data/supplier.tbl
if test -f "$FILE"; then
    echo "$FILE exists."
else
  docker run -v `pwd`/data:/data $TERMINAL_FLAG --rm ghcr.io/scalytics/tpch-docker:main $VERBOSE_OUTPUT -s $1

  # workaround for https://github.com/apache/arrow-datafusion/issues/6147
  mv data/customer.tbl data/customer.csv
  mv data/lineitem.tbl data/lineitem.csv
  mv data/nation.tbl data/nation.csv
  mv data/orders.tbl data/orders.csv
  mv data/part.tbl data/part.csv
  mv data/partsupp.tbl data/partsupp.csv
  mv data/region.tbl data/region.csv
  mv data/supplier.tbl data/supplier.csv

  ls -l data
fi

# Copy expected answers (at SF=1) into the ./data/answers directory if it does not already exist
FILE=./data/answers/q1.out
if test -f "$FILE"; then
    echo "$FILE exists."
else
  docker run -v `pwd`/data:/data $TERMINAL_FLAG --entrypoint /bin/bash --rm ghcr.io/scalytics/tpch-docker:main -c "cp /opt/tpch/2.18.0_rc2/dbgen/answers/* /data/answers/"
fi
