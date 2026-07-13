#!/usr/bin/env bash
set -o nounset
set -o pipefail
set -o xtrace

GCLOUD_REGISTRY="gcr.io/sre-team-418623"

# Parse flags
PUSH=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --push)
      PUSH=true
      shift
      ;;
    *)
      echo "Usage: $0 [--push]"
      echo "  --push    Build and push to ${GCLOUD_REGISTRY}/refinery"
      echo "  (default) Build locally only"
      exit 1
      ;;
  esac
done

VERSION=$(git describe --tags --match='v[0-9]*' --always)
VERSION=${VERSION#v}
GIT_COMMIT=$(git rev-parse HEAD)

unset GOOS
unset GOARCH
export GOFLAGS="-ldflags=-X=main.BuildID=$VERSION"
export SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH:-$(make latest_modification_time)}

# Force IPv4 to avoid IPv6 connectivity issues when pulling base image layers
export GODEBUG=preferIPv4=1

if [[ "$PUSH" == "true" ]]; then
  export KO_DOCKER_REPO="$GCLOUD_REGISTRY"
else
  export KO_DOCKER_REPO="ko.local"
fi

# shellcheck disable=SC2086
IMAGE_REF=$(ko publish \
  --tags "${VERSION}" \
  --base-import-paths \
  --platform "linux/amd64,linux/arm64" \
  --image-label org.opencontainers.image.source=https://github.com/khan/refinery \
  --image-label org.opencontainers.image.licenses=Apache-2.0 \
  --image-label org.opencontainers.image.revision=${GIT_COMMIT} \
  ./cmd/refinery)

echo "Built image: ${IMAGE_REF}"
