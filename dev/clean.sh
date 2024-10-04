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

# This cleans up the project by removing build artifacts and other generated files.

# Function to remove a directory and print the action
remove_dir() {
    if [ -d "$1" ]; then
        echo "Removing directory: $1"
        rm -rf "$1"
    fi
}

# Function to remove a file and print the action
remove_file() {
    if [ -f "$1" ]; then
        echo "Removing file: $1"
        rm -f "$1"
    fi
}

# Remove .pytest_cache directory
remove_dir .pytest_cache/

# Remove target directory
remove_dir target/

# Remove any __pycache__ directories
find python/ -type d -name "__pycache__" -print | while read -r dir; do
    remove_dir "$dir"
done

# Remove pytest-coverage.lcov file
# remove_file .coverage
# remove_file pytest-coverage.lcov

# Remove rust-coverage.lcov file
# remove_file rust-coverage.lcov

# Remove pyo3 files
find python/ -type f -name '_internal.*.so' -print | while read -r file; do
    remove_file "$file"
done

echo "Cleanup complete."