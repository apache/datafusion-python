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

case $# in
  2) VERSION="$1"
     RC_NUMBER="$2"
     ;;
  *) echo "Usage: $0 X.Y.Z RC_NUMBER"
     exit 1
     ;;
esac

set -e
set -x
set -o pipefail

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
DATAFUSION_PYTHON_DIR="$(dirname $(dirname ${SOURCE_DIR}))"
DATAFUSION_PYTHON_DIST_URL='https://dist.apache.org/repos/dist/dev/datafusion'

download_dist_file() {
  curl \
    --silent \
    --show-error \
    --fail \
    --location \
    --remote-name $DATAFUSION_PYTHON_DIST_URL/$1
}

download_rc_file() {
  download_dist_file apache-datafusion-python-${VERSION}-rc${RC_NUMBER}/$1
}

import_gpg_keys() {
  download_dist_file KEYS
  gpg --import KEYS
}

if type shasum >/dev/null 2>&1; then
  sha256_verify="shasum -a 256 -c"
  sha512_verify="shasum -a 512 -c"
else
  sha256_verify="sha256sum -c"
  sha512_verify="sha512sum -c"
fi

fetch_archive() {
  local dist_name=$1
  download_rc_file ${dist_name}.tar.gz
  download_rc_file ${dist_name}.tar.gz.asc
  download_rc_file ${dist_name}.tar.gz.sha256
  download_rc_file ${dist_name}.tar.gz.sha512
  verify_dir_artifact_signatures
}

verify_dir_artifact_signatures() {
  # verify the signature and the checksums of each artifact
  find . -name '*.asc' | while read sigfile; do
    artifact=${sigfile/.asc/}
    gpg --verify $sigfile $artifact || exit 1

    # go into the directory because the checksum files contain only the
    # basename of the artifact
    pushd $(dirname $artifact)
    base_artifact=$(basename $artifact)
    ${sha256_verify} $base_artifact.sha256 || exit 1
    ${sha512_verify} $base_artifact.sha512 || exit 1
    popd
  done
}

setup_tempdir() {
  cleanup() {
    if [ "${TEST_SUCCESS}" = "yes" ]; then
      rm -fr "${DATAFUSION_PYTHON_TMPDIR}"
    else
      echo "Failed to verify release candidate. See ${DATAFUSION_PYTHON_TMPDIR} for details."
    fi
  }

  if [ -z "${DATAFUSION_PYTHON_TMPDIR}" ]; then
    # clean up automatically if DATAFUSION_PYTHON_TMPDIR is not defined
    DATAFUSION_PYTHON_TMPDIR=$(mktemp -d -t "$1.XXXXX")
    trap cleanup EXIT
  else
    # don't clean up automatically
    mkdir -p "${DATAFUSION_PYTHON_TMPDIR}"
  fi
}

test_source_distribution() {
  # install rust toolchain
  export RUSTUP_HOME=$PWD/test-rustup
  export CARGO_HOME=$PWD/test-rustup

  curl https://sh.rustup.rs -sSf | sh -s -- -y --no-modify-path

  # On Unix, rustup creates an env file. On Windows GitHub runners (MSYS bash),
  # that file may not exist, so fall back to adding Cargo bin directly.
  if [ -f "$CARGO_HOME/env" ]; then
    # shellcheck disable=SC1090
    source "$CARGO_HOME/env"
  elif [ -f "$RUSTUP_HOME/env" ]; then
    # shellcheck disable=SC1090
    source "$RUSTUP_HOME/env"
  else
    export PATH="$CARGO_HOME/bin:$PATH"
  fi

  # build and test rust

  # raises on any formatting errors
  rustup component add rustfmt --toolchain stable
  cargo fmt --all -- --check

  # Clone testing repositories into the expected location
  git clone https://github.com/apache/arrow-testing.git testing
  git clone https://github.com/apache/parquet-testing.git parquet-testing

  python3 -m venv .venv
  if [ -x ".venv/bin/python" ]; then
    VENV_PYTHON=".venv/bin/python"
  elif [ -x ".venv/Scripts/python.exe" ]; then
    VENV_PYTHON=".venv/Scripts/python.exe"
  elif [ -x ".venv/Scripts/python" ]; then
    VENV_PYTHON=".venv/Scripts/python"
  else
    echo "Unable to find python executable in virtual environment"
    exit 1
  fi

  "$VENV_PYTHON" -m pip install -U pip
  "$VENV_PYTHON" -m pip install -U maturin
  "$VENV_PYTHON" -m maturin develop

  #TODO: we should really run tests here as well
  #python3 -m pytest

  if ( find -iname 'Cargo.toml' | xargs grep SNAPSHOT ); then
    echo "Cargo.toml version should not contain SNAPSHOT for releases"
    exit 1
  fi
}

TEST_SUCCESS=no

setup_tempdir "datafusion-python-${VERSION}"
echo "Working in sandbox ${DATAFUSION_PYTHON_TMPDIR}"
cd ${DATAFUSION_PYTHON_TMPDIR}

dist_name="apache-datafusion-python-${VERSION}"
import_gpg_keys
fetch_archive ${dist_name}
tar xf ${dist_name}.tar.gz
pushd ${dist_name}
    test_source_distribution
popd

TEST_SUCCESS=yes
echo 'Release candidate looks good!'
exit 0
