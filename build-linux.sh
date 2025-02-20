#!/bin/bash

mkdir -p ./target-linux/wheels

export DOCKER_BUILDKIT=1

root_dir="$(dirname $0)"
script=$(basename $0 .sh)
[ -L $0 ] || {
  >&2 echo "Should be invoked via one of the $root_dir/build-linux-<platform>.sh symlinks"
  exit 1
}
platform="${script#build-linux-}"
case $platform in
  arm64)
    pyplatform=aarch64
    ;;
  *)
    pyplatform=$platform
    ;;
esac

version=$(grep -w '^version' "$root_dir/pyproject.toml" | cut -d\" -f2)
sanitized_version=$(echo $version | tr '+' _)

img_tag=docker-cja-arrow-dev.dr-uw2.adobeitc.com/datafusion-python:$sanitized_version-$platform

docker build \
  --ssh default \
  --progress=plain \
  --platform linux/$platform \
  -f Dockerfile-build-wheel.$platform \
  -t $img_tag \
  "$root_dir"

id=$(docker create --platform linux/$platform $img_tag)
wheel=datafusion-$version-cp38-abi3-manylinux_2_28_$pyplatform.whl
docker cp $id:$wheel .
docker rm -v $id

which jfrog || brew install jfrog-cli

## Artifactory UW2
jfrog rt upload --url https://artifactory-uw2.adobeitc.com/artifactory \
  --user ${ARTIFACTORY_USER:-$ARTIFACTORY_USERNAME} \
  --password ${ARTIFACTORY_UW2_TOKEN:-$ARTIFACTORY_API_TOKEN} \
  $wheel pypi-arrow-release/datafusion/$version/

## Artifactory Corp
#jfrog rt upload --url https://artifactory.corp.adobe.com/artifactory \
#  --user ${ARTIFACTORY_USER:-$ARTIFACTORY_USERNAME} \
#  --password ${ARTIFACTORY_CORP_TOKEN:-$ARTIFACTORY_API_TOKEN} \
#  $wheel pypi-arrow-release/datafusion/$version/

