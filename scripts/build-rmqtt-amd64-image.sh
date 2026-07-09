#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "用法：$0 <完整镜像名称:tag>" >&2
  exit 2
fi

IMAGE="$1"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${ROOT}/target/x86_64-unknown-linux-musl/release/rmqttd"
BUILD_ID="${RMQTT_BUILD_ID:-$(date '+%Y%m%d%H%M%S')}"
CARGO_REGISTRY_CACHE="${RMQTT_CARGO_REGISTRY_CACHE:-/private/tmp/rmqtt-cargo-registry-amd64}"
CARGO_GIT_CACHE="${RMQTT_CARGO_GIT_CACHE:-/private/tmp/rmqtt-cargo-git-amd64}"

echo "==> 编译 RMQTT，build id=${BUILD_ID}"
docker run --rm --platform linux/amd64 \
  -e "RMQTT_BUILD_ID=${BUILD_ID}" \
  -v "${ROOT}:/src" \
  -v "${CARGO_REGISTRY_CACHE}:/usr/local/cargo/registry" \
  -v "${CARGO_GIT_CACHE}:/usr/local/cargo/git" \
  -w /src \
  rust:1.89-alpine \
  sh -lc '. /usr/local/cargo/env && apk add --no-cache build-base cmake openssl-dev openssl-libs-static perl pkgconf protobuf-dev zlib-static && rustup target add x86_64-unknown-linux-musl && cargo build --release --target x86_64-unknown-linux-musl -p rmqttd'

if [[ ! -x "${BINARY}" ]]; then
  echo "错误：没有生成 ${BINARY}" >&2
  exit 1
fi

SOURCE_PATHS=(
  Cargo.toml
  Cargo.lock
  rmqtt
  rmqtt-bin
  rmqtt-codec
  rmqtt-conf
  rmqtt-macros
  rmqtt-net
  rmqtt-plugins
  rmqtt-utils
)

NEWER_SOURCE="$(
  cd "${ROOT}"
  find "${SOURCE_PATHS[@]}" -type f \
    \( -name '*.rs' -o -name 'Cargo.toml' -o -name 'Cargo.lock' -o -name 'build.rs' \) \
    -newer "${BINARY}" -print -quit
)"
if [[ -n "${NEWER_SOURCE}" ]]; then
  echo "错误：release binary 比源码旧：${NEWER_SOURCE}" >&2
  echo "拒绝继续构建镜像，避免把旧 target/rmqttd 打进去。" >&2
  exit 1
fi

echo "==> 构建镜像 ${IMAGE}"
docker build --platform linux/amd64 \
  --build-arg "RMQTT_BUILD_ID=${BUILD_ID}" \
  -f "${ROOT}/Dockerfile.amd64" \
  -t "${IMAGE}" \
  "${ROOT}"

echo "==> 校验镜像中的二进制版本"
IMAGE_VERSION="$(
  docker run --rm --platform linux/amd64 \
    --entrypoint /app/rmqtt/rmqtt-bin/rmqttd \
    "${IMAGE}" \
    --version
)"
echo "${IMAGE_VERSION}"
if [[ "${IMAGE_VERSION}" != *"-${BUILD_ID} "* && "${IMAGE_VERSION}" != *"-${BUILD_ID}" ]]; then
  echo "错误：镜像内 RMQTT build id 与本次编译不一致，期望 ${BUILD_ID}" >&2
  exit 1
fi

echo "==> 构建及校验完成"
echo "镜像：${IMAGE}"
echo "推送命令：docker push ${IMAGE}"
