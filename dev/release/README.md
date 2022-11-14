<!---
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

# DataFusion Python Release Process

This is a work-in-progress that will be updated as we work through the next release.

## Preparing a Release Candidate

- Update the version number in Cargo.toml
- Generate changelog
- Tag the repo with an rc tag e.g. `0.7.0-rc1`
- Create tarball and upload to ASF
- Start the vote

## Releasing Artifacts

```bash
maturin publish
```