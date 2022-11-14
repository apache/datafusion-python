#!/bin/bash
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

# Adapted from https://github.com/apache/arrow-rs/tree/master/dev/release/update_change_log.sh

# invokes the changelog generator from
# https://github.com/github-changelog-generator/github-changelog-generator
#
# With the config located in
# arrow-datafusion/.github_changelog_generator
#
# Usage:
# CHANGELOG_GITHUB_TOKEN=<TOKEN> ./update_change_log.sh <PROJECT> <SINCE_TAG> <EXTRA_ARGS...>

set -e

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

if [[ "$#" -lt 1 ]]; then
    echo "USAGE: $0 SINCE_TAG EXTRA_ARGS..."
    exit 1
fi

SINCE_TAG=$1
shift 1

OUTPUT_PATH="CHANGELOG.md"

pushd ${SOURCE_TOP_DIR}

# reset content in changelog
git checkout "${SINCE_TAG}" "${OUTPUT_PATH}"
# remove license header so github-changelog-generator has a clean base to append
sed -i.bak '1,18d' "${OUTPUT_PATH}"

docker run -it --rm \
    --cpus "0.1" \
    -e CHANGELOG_GITHUB_TOKEN=$CHANGELOG_GITHUB_TOKEN \
    -v "$(pwd)":/usr/local/src/your-app \
    githubchangeloggenerator/github-changelog-generator \
    --user apache \
    --project arrow-datafusion-python \
    --since-tag "${SINCE_TAG}" \
    --base "${OUTPUT_PATH}" \
    --output "${OUTPUT_PATH}" \
    "$@"

sed -i.bak "s/\\\n/\n\n/" "${OUTPUT_PATH}"

echo '<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
' | cat - "${OUTPUT_PATH}" > "${OUTPUT_PATH}".tmp
mv "${OUTPUT_PATH}".tmp "${OUTPUT_PATH}"
