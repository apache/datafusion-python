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

Development happens on the `main` branch, and most of the time, we depend on DataFusion using GitHub dependencies
rather than using an official release from crates.io. This allows us to pick up new features and bug fixes frequently
by creating PRs to move to a later revision of the code. It also means we can incrementally make updates that are
required due to changes in DataFusion rather than having a large amount of work to do when the next official release
is available.

When there is a new official release of DataFusion, we update the `main` branch to point to that, update the version
number, and create a new release branch, such as `branch-0.8`. Once this branch is created, we switch the `main` branch
back to using GitHub dependencies. The release activity (such as generating the changelog) can then happen on the
release branch without blocking ongoing development in the `main` branch.

We can cherry-pick commits from the `main` branch into `branch-0.8` as needed and then create new patch releases
from that branch.

## Detailed Guide

### Pre-requisites

Releases can currently only be created by PMC members due to the permissions needed.

You will need a GitHub Personal Access Token. Follow
[these instructions](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
to generate one if you do not already have one.

You will need a PyPI API token. Create one at https://test.pypi.org/manage/account/#api-tokens, setting the “Scope” to
“Entire account”.

You will also need access to the [datafusion](https://test.pypi.org/project/datafusion/) project on testpypi.

### Preparing the `main` Branch

Before creating a new release:

- We need to ensure that the main branch does not have any GitHub dependencies
- a PR should be created and merged to update the major version number of the project
- A new release branch should be created, such as `branch-0.8`

## Preparing a Release Candidate

### Change Log

We maintain a `CHANGELOG.md` so our users know what has been changed between releases.

The changelog is generated using a Python script:

```bash
$ GITHUB_TOKEN=<TOKEN> ./dev/release/generate-changelog.py 24.0.0 HEAD 25.0.0 > dev/changelog/25.0.0.md
```

This script creates a changelog from GitHub PRs based on the labels associated with them as well as looking for
titles starting with `feat:`, `fix:`, or `docs:` . The script will produce output similar to:

```
Fetching list of commits between 24.0.0 and HEAD
Fetching pull requests
Categorizing pull requests
Generating changelog content
```

### Update the version number

The only place you should need to update the version is in the root `Cargo.toml`.
After updating the toml file, run `cargo update` to update the cargo lock file.
If you do not want to update all the dependencies, you can instead run `cargo build`
which should only update the version number for `datafusion-python`.

### Tag the Repository

Commit the changes to the changelog and version.

Assuming you have set up a remote to the `apache` repository rather than your personal fork,
you need to push a tag to start the CI process for release candidates. The following assumes
the upstream repository is called `apache`.

```bash
git tag 0.8.0-rc1
git push apache 0.8.0-rc1
```

### Create a source release

```bash
./dev/release/create-tarball.sh 0.8.0 1
```

This will also create the email template to send to the mailing list.

Create a draft email using this content, but do not send until after completing the next step.

### Publish Python Artifacts to testpypi

This section assumes some familiarity with publishing Python packages to PyPi. For more information, refer to \
[this tutorial](https://packaging.python.org/en/latest/tutorials/packaging-projects/#uploading-the-distribution-archives).

#### Publish Python Wheels to testpypi

Pushing an `rc` tag to the release branch will cause a GitHub Workflow to run that will build the Python wheels.

Go to https://github.com/apache/datafusion-python/actions and look for an action named "Python Release Build"
that has run against the pushed tag.

Click on the action and scroll down to the bottom of the page titled "Artifacts". Download `dist.zip`. It should
contain files such as:

```text
datafusion-22.0.0-cp37-abi3-macosx_10_7_x86_64.whl
datafusion-22.0.0-cp37-abi3-macosx_11_0_arm64.whl
datafusion-22.0.0-cp37-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
datafusion-22.0.0-cp37-abi3-win_amd64.whl
```

Upload the wheels to testpypi.

```bash
unzip dist.zip
python3 -m pip install --upgrade setuptools twine build
python3 -m twine upload --repository testpypi datafusion-22.0.0-cp37-abi3-*.whl
```

When prompted for username, enter `__token__`. When prompted for a password, enter a valid GitHub Personal Access Token

#### Publish Python Source Distribution to testpypi

Download the source tarball created in the previous step, untar it, and run:

```bash
maturin sdist
```

This will create a file named `dist/datafusion-0.7.0.tar.gz`. Upload this to testpypi:

```bash
python3 -m twine upload --repository testpypi dist/datafusion-0.7.0.tar.gz
```

### Run Verify Release Candidate Workflow

Before sending the vote email, run the manually triggered GitHub Actions workflow
"Verify Release Candidate" and confirm all matrix jobs pass across the OS/architecture matrix
(for example, Linux, macOS, and Windows runners):

1. Go to https://github.com/apache/datafusion-python/actions/workflows/verify-release-candidate.yml
2. Click "Run workflow"
3. Set `version` to the release version (for example, `52.0.0`)
4. Set `rc_number` to the RC number (for example, `0`)
5. Wait for all jobs to complete successfully

Include a short note in the vote email template that this workflow was run across all OS/architecture
matrix entries and that all jobs passed.

```text
Verification note: The manually triggered "Verify Release Candidate" workflow was run for version <VERSION> and rc_number <RC_NUMBER> across all configured OS/architecture matrix entries, and all matrix jobs completed successfully.
```

### Send the Email

Send the email to start the vote.

## Verifying a Release

Releases may be verified using `verify-release-candidate.sh`:

```bash
git clone https://github.com/apache/datafusion-python.git
dev/release/verify-release-candidate.sh 48.0.0 1
```

Alternatively, one can run unit tests against a testpypi release candidate:

```bash
# clone a fresh repo
git clone https://github.com/apache/datafusion-python.git
cd datafusion-python

# checkout the release commit
git fetch --tags
git checkout 40.0.0-rc1
git submodule update --init --recursive

# create the env
python3 -m venv .venv
source .venv/bin/activate

# install release candidate
pip install --extra-index-url https://test.pypi.org/simple/ datafusion==40.0.0

# install test dependencies
pip install pytest numpy pytest-asyncio

# run the tests
pytest --import-mode=importlib python/tests -vv
```

Try running one of the examples from the top-level README, or write some custom Python code to query some available
data files.

## Publishing a Release

### Publishing Apache Source Release

Once the vote passes, we can publish the release.

Create the source release tarball:

```bash
./dev/release/release-tarball.sh 0.8.0 1
```

### Publishing Rust Crate to crates.io

Some projects depend on the Rust crate directly, so we publish this to crates.io

```shell
cargo publish
```

### Publishing Python Artifacts to PyPi

Go to the Test PyPI page of Datafusion, and download
[all published artifacts](https://test.pypi.org/project/datafusion/#files) under `dist-release/` directory. Then proceed
uploading them using `twine`:

```bash
twine upload --repository pypi dist-release/*
```

### Publish Python Artifacts to conda-forge

Pypi packages auto upload to conda-forge via [datafusion feedstock](https://github.com/conda-forge/datafusion-feedstock)

### Push the Release Tag

```bash
git checkout 0.8.0-rc1
git tag 0.8.0
git push apache 0.8.0
```

### Add the release to Apache Reporter

Add the release to https://reporter.apache.org/addrelease.html?datafusion with a version name prefixed with `DATAFUSION-PYTHON`,
for example `DATAFUSION-PYTHON-31.0.0`.

The release information is used to generate a template for a board report (see example from Apache Arrow
[here](https://github.com/apache/arrow/pull/14357)).

### Delete old RCs and Releases

See the ASF documentation on [when to archive](https://www.apache.org/legal/release-policy.html#when-to-archive)
for more information.

#### Deleting old release candidates from `dev` svn

Release candidates should be deleted once the release is published.

Get a list of DataFusion release candidates:

```bash
svn ls https://dist.apache.org/repos/dist/dev/datafusion | grep datafusion-python
```

Delete a release candidate:

```bash
svn delete -m "delete old DataFusion RC" https://dist.apache.org/repos/dist/dev/datafusion/apache-datafusion-python-7.1.0-rc1/
```

#### Deleting old releases from `release` svn

Only the latest release should be available. Delete old releases after publishing the new release.

Get a list of DataFusion releases:

```bash
svn ls https://dist.apache.org/repos/dist/release/datafusion | grep datafusion-python
```

Delete a release:

```bash
svn delete -m "delete old DataFusion release" https://dist.apache.org/repos/dist/release/datafusion/datafusion-python-7.0.0
```
