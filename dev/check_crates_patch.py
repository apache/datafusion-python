#!/usr/bin/env python3
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

"""Check that no Cargo.toml files contain [patch.crates-io] entries.

Release builds must not depend on patched crates. During development it is
common to temporarily patch crates-io dependencies, but those patches must
be removed before creating a release.

An empty [patch.crates-io] section is allowed.
"""

import sys
from pathlib import Path

import tomllib


def main() -> int:
    errors: list[str] = []
    for cargo_toml in sorted(Path().rglob("Cargo.toml")):
        if "target" in cargo_toml.parts:
            continue
        with Path.open(cargo_toml, "rb") as f:
            data = tomllib.load(f)
        patch = data.get("patch", {}).get("crates-io", {})
        if patch:
            errors.append(str(cargo_toml))
            for name, spec in patch.items():
                errors.append(f"  {name} = {spec}")

    if errors:
        print("ERROR: Release builds must not contain [patch.crates-io] entries.")
        print()
        for line in errors:
            print(line)
        print()
        print("Remove all [patch.crates-io] entries before creating a release.")
        return 1

    print("OK: No [patch.crates-io] entries found.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
