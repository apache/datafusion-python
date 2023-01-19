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

# Changelog

## [0.8.0](https://github.com/apache/arrow-datafusion-python/tree/0.8.0) (2023-01-19)

[Full Changelog](https://github.com/apache/arrow-datafusion-python/compare/0.7.0...0.8.0)

**Implemented enhancements:**

- Add script for Python linting [\#134](https://github.com/apache/arrow-datafusion-python/issues/134)
- support creating arrow-datafusion-python conda environment [\#122](https://github.com/apache/arrow-datafusion-python/issues/122)
- Build Python source distribution in GitHub workflow [\#81](https://github.com/apache/arrow-datafusion-python/issues/81)

**Fixed bugs:**

- Reading csv does not work [\#130](https://github.com/apache/arrow-datafusion-python/issues/130)
- Github actions produce a lot of warnings [\#94](https://github.com/apache/arrow-datafusion-python/issues/94)
- ASF source release tarball has wrong directory name [\#90](https://github.com/apache/arrow-datafusion-python/issues/90)
- Python Release Build failing after upgrading to maturin 14.2 [\#87](https://github.com/apache/arrow-datafusion-python/issues/87)
- Maturin build hangs on Linux ARM64 [\#84](https://github.com/apache/arrow-datafusion-python/issues/84)
- Cannot install on Mac M1 from source tarball from testpypi [\#82](https://github.com/apache/arrow-datafusion-python/issues/82)
- ImportPathMismatchError when running pytest locally [\#77](https://github.com/apache/arrow-datafusion-python/issues/77)

**Closed issues:**

- Publish documentation for Python bindings [\#39](https://github.com/apache/arrow-datafusion-python/issues/39)

**Merged pull requests:**

- Improve README and add more examples [\#137](https://github.com/apache/arrow-datafusion-python/pull/137) ([andygrove](https://github.com/andygrove))
- build\(deps\): bump object\_store from 0.5.2 to 0.5.3 [\#126](https://github.com/apache/arrow-datafusion-python/pull/126) ([dependabot[bot]](https://github.com/apps/dependabot))
- build\(deps\): bump mimalloc from 0.1.32 to 0.1.34 [\#125](https://github.com/apache/arrow-datafusion-python/pull/125) ([dependabot[bot]](https://github.com/apps/dependabot))
- Introduce conda directory containing datafusion-dev.yaml conda enviro… [\#124](https://github.com/apache/arrow-datafusion-python/pull/124) ([jdye64](https://github.com/jdye64))
- build\(deps\): bump bzip2 from 0.4.3 to 0.4.4 [\#121](https://github.com/apache/arrow-datafusion-python/pull/121) ([dependabot[bot]](https://github.com/apps/dependabot))
- build\(deps\): bump tokio from 1.23.0 to 1.24.1 [\#119](https://github.com/apache/arrow-datafusion-python/pull/119) ([dependabot[bot]](https://github.com/apps/dependabot))
- build\(deps\): bump async-trait from 0.1.60 to 0.1.61 [\#118](https://github.com/apache/arrow-datafusion-python/pull/118) ([dependabot[bot]](https://github.com/apps/dependabot))
- Upgrade to DataFusion 16.0.0 [\#115](https://github.com/apache/arrow-datafusion-python/pull/115) ([andygrove](https://github.com/andygrove))
- Bump async-trait from 0.1.57 to 0.1.60 [\#114](https://github.com/apache/arrow-datafusion-python/pull/114) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump object\_store from 0.5.1 to 0.5.2 [\#112](https://github.com/apache/arrow-datafusion-python/pull/112) ([dependabot[bot]](https://github.com/apps/dependabot))
- Bump tokio from 1.21.2 to 1.23.0 [\#109](https://github.com/apache/arrow-datafusion-python/pull/109) ([dependabot[bot]](https://github.com/apps/dependabot))
- Add entries for publishing production \(asf-site\) and staging docs [\#107](https://github.com/apache/arrow-datafusion-python/pull/107) ([martin-g](https://github.com/martin-g))
- Add a workflow that builds the docs and deploys them at staged or production [\#104](https://github.com/apache/arrow-datafusion-python/pull/104) ([martin-g](https://github.com/martin-g))
- Upgrade to DataFusion 15.0.0 [\#103](https://github.com/apache/arrow-datafusion-python/pull/103) ([andygrove](https://github.com/andygrove))
- build\(deps\): bump futures from 0.3.24 to 0.3.25 [\#102](https://github.com/apache/arrow-datafusion-python/pull/102) ([dependabot[bot]](https://github.com/apps/dependabot))
- build\(deps\): bump pyo3 from 0.17.2 to 0.17.3 [\#101](https://github.com/apache/arrow-datafusion-python/pull/101) ([dependabot[bot]](https://github.com/apps/dependabot))
- build\(deps\): bump mimalloc from 0.1.30 to 0.1.32 [\#98](https://github.com/apache/arrow-datafusion-python/pull/98) ([dependabot[bot]](https://github.com/apps/dependabot))
- build\(deps\): bump rand from 0.7.3 to 0.8.5 [\#97](https://github.com/apache/arrow-datafusion-python/pull/97) ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix GitHub actions warnings [\#95](https://github.com/apache/arrow-datafusion-python/pull/95) ([martin-g](https://github.com/martin-g))
- Fixes \#81 - Add CI workflow for source distribution [\#93](https://github.com/apache/arrow-datafusion-python/pull/93) ([martin-g](https://github.com/martin-g))
- post-release updates [\#91](https://github.com/apache/arrow-datafusion-python/pull/91) ([andygrove](https://github.com/andygrove))
- Build for manylinux 2014 [\#88](https://github.com/apache/arrow-datafusion-python/pull/88) ([martin-g](https://github.com/martin-g))
- update release readme tag [\#86](https://github.com/apache/arrow-datafusion-python/pull/86) ([Jimexist](https://github.com/Jimexist))
- Upgrade Maturin to 0.14.2 [\#85](https://github.com/apache/arrow-datafusion-python/pull/85) ([martin-g](https://github.com/martin-g))
- Update release instructions [\#83](https://github.com/apache/arrow-datafusion-python/pull/83) ([andygrove](https://github.com/andygrove))
- \[Functions\] - Add python function binding to `functions` [\#73](https://github.com/apache/arrow-datafusion-python/pull/73) ([francis-du](https://github.com/francis-du))


## [Unreleased](https://github.com/datafusion-contrib/datafusion-python/tree/HEAD)

[Full Changelog](https://github.com/datafusion-contrib/datafusion-python/compare/0.5.1...HEAD)

**Merged pull requests:**

- use \_\_getitem\_\_ for df column selection [\#41](https://github.com/datafusion-contrib/datafusion-python/pull/41) ([Jimexist](https://github.com/Jimexist))
- fix demo in readme [\#40](https://github.com/datafusion-contrib/datafusion-python/pull/40) ([Jimexist](https://github.com/Jimexist))
- Implement select_columns [\#39](https://github.com/datafusion-contrib/datafusion-python/pull/39) ([andygrove](https://github.com/andygrove))
- update readme and changelog [\#38](https://github.com/datafusion-contrib/datafusion-python/pull/38) ([Jimexist](https://github.com/Jimexist))
- Add PyDataFrame.explain [\#36](https://github.com/datafusion-contrib/datafusion-python/pull/36) ([andygrove](https://github.com/andygrove))
- Release 0.5.0 [\#34](https://github.com/datafusion-contrib/datafusion-python/pull/34) ([Jimexist](https://github.com/Jimexist))
- disable nightly in workflow [\#33](https://github.com/datafusion-contrib/datafusion-python/pull/33) ([Jimexist](https://github.com/Jimexist))
- update requirements to 37 and 310, update readme [\#32](https://github.com/datafusion-contrib/datafusion-python/pull/32) ([Jimexist](https://github.com/Jimexist))
- Add custom global allocator [\#30](https://github.com/datafusion-contrib/datafusion-python/pull/30) ([matthewmturner](https://github.com/matthewmturner))
- Remove pandas dependency [\#25](https://github.com/datafusion-contrib/datafusion-python/pull/25) ([matthewmturner](https://github.com/matthewmturner))
- upgrade datafusion and pyo3 [\#20](https://github.com/datafusion-contrib/datafusion-python/pull/20) ([Jimexist](https://github.com/Jimexist))
- update maturin 0.12+ [\#17](https://github.com/datafusion-contrib/datafusion-python/pull/17) ([Jimexist](https://github.com/Jimexist))
- Update README.md [\#16](https://github.com/datafusion-contrib/datafusion-python/pull/16) ([Jimexist](https://github.com/Jimexist))
- apply cargo clippy --fix [\#15](https://github.com/datafusion-contrib/datafusion-python/pull/15) ([Jimexist](https://github.com/Jimexist))
- update test workflow to include rust clippy and check [\#14](https://github.com/datafusion-contrib/datafusion-python/pull/14) ([Jimexist](https://github.com/Jimexist))
- use maturin 0.12.6 [\#13](https://github.com/datafusion-contrib/datafusion-python/pull/13) ([Jimexist](https://github.com/Jimexist))
- apply cargo fmt [\#12](https://github.com/datafusion-contrib/datafusion-python/pull/12) ([Jimexist](https://github.com/Jimexist))
- use stable not nightly [\#11](https://github.com/datafusion-contrib/datafusion-python/pull/11) ([Jimexist](https://github.com/Jimexist))
- ci: test against more compilers, setup clippy and fix clippy lints [\#9](https://github.com/datafusion-contrib/datafusion-python/pull/9) ([cpcloud](https://github.com/cpcloud))
- Fix use of importlib.metadata and unify requirements.txt [\#8](https://github.com/datafusion-contrib/datafusion-python/pull/8) ([cpcloud](https://github.com/cpcloud))
- Ship the Cargo.lock file in the source distribution [\#7](https://github.com/datafusion-contrib/datafusion-python/pull/7) ([cpcloud](https://github.com/cpcloud))
- add \_\_version\_\_ attribute to datafusion object [\#3](https://github.com/datafusion-contrib/datafusion-python/pull/3) ([tfeda](https://github.com/tfeda))
- fix ci by fixing directories [\#2](https://github.com/datafusion-contrib/datafusion-python/pull/2) ([Jimexist](https://github.com/Jimexist))
- setup workflow [\#1](https://github.com/datafusion-contrib/datafusion-python/pull/1) ([Jimexist](https://github.com/Jimexist))

## [0.5.1](https://github.com/datafusion-contrib/datafusion-python/tree/0.5.1) (2022-03-15)

[Full Changelog](https://github.com/datafusion-contrib/datafusion-python/compare/0.5.1-rc1...0.5.1)

## [0.5.1-rc1](https://github.com/datafusion-contrib/datafusion-python/tree/0.5.1-rc1) (2022-03-15)

[Full Changelog](https://github.com/datafusion-contrib/datafusion-python/compare/0.5.0...0.5.1-rc1)

## [0.5.0](https://github.com/datafusion-contrib/datafusion-python/tree/0.5.0) (2022-03-10)

[Full Changelog](https://github.com/datafusion-contrib/datafusion-python/compare/0.5.0-rc2...0.5.0)

## [0.5.0-rc2](https://github.com/datafusion-contrib/datafusion-python/tree/0.5.0-rc2) (2022-03-10)

[Full Changelog](https://github.com/datafusion-contrib/datafusion-python/compare/0.5.0-rc1...0.5.0-rc2)

**Closed issues:**

- Add support for Ballista [\#37](https://github.com/datafusion-contrib/datafusion-python/issues/37)
- Implement DataFrame.explain [\#35](https://github.com/datafusion-contrib/datafusion-python/issues/35)

## [0.5.0-rc1](https://github.com/datafusion-contrib/datafusion-python/tree/0.5.0-rc1) (2022-03-09)

[Full Changelog](https://github.com/datafusion-contrib/datafusion-python/compare/4c98b8e9c3c3f8e2e6a8f2d1ffcfefda344c4680...0.5.0-rc1)

**Closed issues:**

- Investigate exposing additional optimizations [\#28](https://github.com/datafusion-contrib/datafusion-python/issues/28)
- Use custom allocator in Python build [\#27](https://github.com/datafusion-contrib/datafusion-python/issues/27)
- Why is pandas a requirement? [\#24](https://github.com/datafusion-contrib/datafusion-python/issues/24)
- Unable to build [\#18](https://github.com/datafusion-contrib/datafusion-python/issues/18)
- Setup CI against multiple Python version [\#6](https://github.com/datafusion-contrib/datafusion-python/issues/6)

\* _This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)_


\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
