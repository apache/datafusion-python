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

original_dir=$(pwd)
script_dir=$(dirname "$(realpath "$0")")
cd "$script_dir" || exit

if [ ! -f pokemon.csv ]; then
    curl -O https://gist.githubusercontent.com/ritchie46/cac6b337ea52281aa23c049250a4ff03/raw/89a957ff3919d90e6ef2d34235e6bf22304f3366/pokemon.csv
fi

if [ ! -f yellow_tripdata_2021-01.parquet ]; then
    curl -O https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
fi

rm -rf build 2> /dev/null
rm -rf temp 2> /dev/null
rm -rf ipython 2> /dev/null
mkdir temp
cp -rf source/* temp/

# Give the notebook kernels a writable IPython profile of our own, and put
# `ipython_kernel_config.py` in it. Two reasons, both about build output:
#
#   * Without a writable IPYTHONDIR, IPython warns "IPython parent '<home>' is
#     not a writable location, using a temp directory" once per kernel start.
#   * The config file filters ipykernel's unencrypted-TCP notice, which
#     otherwise repeats once per executed page. See the comments in it.
#
# `IPKernelApp` reads the IPython profile directory rather than the Jupyter
# config path, so this has to be an IPYTHONDIR profile and not a
# `JUPYTER_CONFIG_PATH` entry. Kept out of temp/, which is the Sphinx source
# directory for this build.
export IPYTHONDIR="$script_dir/ipython"
mkdir -p "$IPYTHONDIR/profile_default"
cp ipython_kernel_config.py "$IPYTHONDIR/profile_default/"

# myst-nb executes each page as a notebook from the directory that page
# lives in, so the example data files must sit alongside every page that
# loads them by relative name (e.g. `ctx.read_csv("pokemon.csv")`). Symlink
# them into each directory that has such a page rather than copying the
# 20 MB parquet repeatedly.
for d in temp temp/user-guide temp/user-guide/common-operations; do
    ln -sf "$script_dir/pokemon.csv" "$d/pokemon.csv"
    ln -sf "$script_dir/yellow_tripdata_2021-01.parquet" "$d/yellow_tripdata_2021-01.parquet"
done

# myst-nb runs `{code-cell}` blocks against a Jupyter kernel named "python3".
# Register the active environment's interpreter as that kernel (idempotent).
python -m ipykernel install --sys-prefix --name python3 --display-name "Python 3"

make SOURCEDIR=`pwd`/temp html

cd "$original_dir" || exit
