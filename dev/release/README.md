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