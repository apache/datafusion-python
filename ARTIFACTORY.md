# Publish to artifactory

## MacOS binary
```shell
maturin publish --repository-url https://artifactory-uw2.adobeitc.com/artifactory/api/pypi/pypi-arrow-release/ --password $ARTIFACTORY_UW2_TOKEN --username $ARTIFACTORY_USER
```

## Linux ARM64
```shell
./build-linux-arm64.sh
```

## Linux X86_64
```shell
./build-linux-x86_64.sh
```