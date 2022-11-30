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

## Update Version

The version number in Cargo.toml should be increased, according to semver.

## Update CHANGELOG.md

Define release branch (e.g. `master`), base version tag (e.g. `0.6.0`) and future version tag (e.g. `0.7.0`). Commits
between the base version tag and the release branch will be used to populate the changelog content.

You will need a GitHub Personal Access Token for the following steps. Follow
[these instructions](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
to generate one if you do not already have one.

```bash
# create the changelog
CHANGELOG_GITHUB_TOKEN=<TOKEN> ./dev/release/update_change_log-datafusion-python.sh master 0.7.0 0.6.0
# review change log / edit issues and labels if needed, rerun until you are happy with the result
git commit -a -m 'Create changelog for release'
```

_If you see the error `"You have exceeded a secondary rate limit"` when running this script, try reducing the CPU
allocation to slow the process down and throttle the number of GitHub requests made per minute, by modifying the
value of the `--cpus` argument in the `update_change_log.sh` script._

You can add `invalid` or `development-process` label to exclude items from
release notes.

Send a PR to get these changes merged into `master` branch. If new commits that
could change the change log content landed in the `master` branch before you
could merge the PR, you need to rerun the changelog update script to regenerate
the changelog and update the PR accordingly.

## Preparing a Release Candidate

### Tag the Repository

```bash
git tag 0.7.0-rc1
git push apache 0.7.0-rc1
```

### Create a source release

```bash
./dev/release/create_tarball 0.7.0 1
```

This will also create the email template to send to the mailing list. Here is an example:

```
To: dev@arrow.apache.org
Subject: [VOTE][RUST][DataFusion] Release DataFusion Python Bindings 0.7.0 RC2
Hi,

I would like to propose a release of Apache Arrow DataFusion Python Bindings,
version 0.7.0.

This release candidate is based on commit: bd1b78b6d444b7ab172c6aec23fa58c842a592d7 [1]
The proposed release tarball and signatures are hosted at [2].
The changelog is located at [3].
The Python wheels are located at [4].

Please download, verify checksums and signatures, run the unit tests, and vote
on the release. The vote will be open for at least 72 hours.

Only votes from PMC members are binding, but all members of the community are
encouraged to test the release and vote with "(non-binding)".

The standard verification procedure is documented at https://github.com/apache/arrow-datafusion-python/blob/master/dev/release/README.md#verifying-release-candidates.

[ ] +1 Release this as Apache Arrow DataFusion Python 0.7.0
[ ] +0
[ ] -1 Do not release this as Apache Arrow DataFusion Python 0.7.0 because...

Here is my vote:

+1

[1]: https://github.com/apache/arrow-datafusion-python/tree/bd1b78b6d444b7ab172c6aec23fa58c842a592d7
[2]: https://dist.apache.org/repos/dist/dev/arrow/apache-arrow-datafusion-python-0.7.0-rc2
[3]: https://github.com/apache/arrow-datafusion-python/blob/bd1b78b6d444b7ab172c6aec23fa58c842a592d7/CHANGELOG.md
[4]: https://test.pypi.org/project/datafusion/0.7.0/
```

Create a draft email using this content, but do not send until after completing the next step.

### Publish Python Artifacts to testpypi

To securely upload your project, you’ll need a PyPI API token. Create one at
https://test.pypi.org/manage/account/#api-tokens, setting the “Scope” to “Entire account”.

You will also need access to the [datafusion](https://test.pypi.org/project/datafusion/) project on testpypi.

This section assumes some familiary with publishing Python packages to PyPi. For more information, refer to \
[this tutorial](https://packaging.python.org/en/latest/tutorials/packaging-projects/#uploading-the-distribution-archives).

#### Publish Python Wheels to testpypi

Pushing an `rc` tag to master will cause a GitHub Workflow to run that will build the Python wheels.

Go to https://github.com/apache/arrow-datafusion-python/actions and look for an action named "Python Release Build"
that has run against the pushed tag.

Click on the action and scroll down to the bottom of the page titled "Artifacts". Download `dist.zip`.

Upload the wheels to testpypi.

```bash
unzip dist.zip
python3 -m pip install --upgrade setuptools twine build
python3 -m twine upload --repository testpypi datafusion-0.7.0-cp37-abi3-*.whl
```

When prompted for username, enter `__token__`. When prompted for a password, enter a valid GitHub Personal Access Token

#### Publish Python Source Distribution to testpypi

Download the source tarball created in the previous step, untar it, and run:

```bash
python3 -m build
```

This will create a file named `dist/datafusion-0.7.0.tar.gz`. Upload this to testpypi:

```bash
python3 -m twine upload --repository testpypi dist/datafusion-0.7.0.tar.gz
```

### Send the Email

Send the email to start the vote.

## Verifying a Release

Install the release from testpypi:

```bash
pip install --extra-index-url https://test.pypi.org/simple/ datafusion==0.7.0
```

Try running one of the examples from the top-level README, or write some custom Python code to query some available
data files.

## Publishing a Release

### Publishing Apache Source Release

Once the vote passes, we can publish the release.

Create the source release tarball:

```bash
./dev/release/release-tarball.sh 0.7.0 1
```

### Publishing Python Artifacts

Go to the Test PyPI page of Datafusion, and download
[all published artifacts](https://test.pypi.org/project/datafusion/#files) under `dist-release/` directory. Then proceed
uploading them using `twine`:

```bash
twine upload --repository pypi dist-release/*
```

### Push the Release Tag

```bash
git checkout 0.7.0-rc1
git tag 0.7.0
git push apache 0.7.0
```
