#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
rust_image='rust:1.88.0-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0'
python_image='python:3.12.10-slim-bookworm@sha256:fd95fa221297a88e1cf49c55ec1828edd7c5a428187e67b5d1805692d11588db'
target_volume="alpaca-autotrader-pyo3-check-$$"

cleanup() {
  docker volume rm --force "$target_volume" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

if ! command -v docker >/dev/null 2>&1; then
  printf 'Compiled PyO3 parity check requires Docker\n' >&2
  exit 1
fi

docker volume create "$target_volume" >/dev/null

# The source is read-only. Cargo.lock, the pinned toolchain image, and the
# isolated target volume make this the same build path locally and in CI.
docker run --rm \
  --cap-drop=ALL \
  --security-opt=no-new-privileges:true \
  --tmpfs /tmp:rw,nosuid,nodev,size=64m \
  --tmpfs /cargo-home:rw,exec,nosuid,nodev,size=1g \
  --mount "type=bind,src=${repo_root},dst=/workspace,readonly" \
  --mount "type=volume,src=${target_volume},dst=/target" \
  --env CARGO_HOME=/cargo-home \
  --env CARGO_TARGET_DIR=/target \
  --workdir /workspace \
  "$rust_image" \
  cargo build --locked \
    --package alpaca-autotrader-py \
    --package alpaca-autotrader

# The extension is abi3, but the research runtime is deliberately pinned to
# Python 3.12. Copying only into container-local /tmp proves the checked-out
# source tree cannot supply a Python fallback or shadow the compiled module.
docker run --rm \
  --cap-drop=ALL \
  --security-opt=no-new-privileges:true \
  --network none \
  --read-only \
  --tmpfs /tmp:rw,exec,nosuid,nodev,size=64m \
  --mount "type=bind,src=${repo_root},dst=/workspace,readonly" \
  --mount "type=volume,src=${target_volume},dst=/target,readonly" \
  --workdir /workspace \
  "$python_image" \
  sh -eu -c '
    mkdir -p /tmp/compiled-extension
    cp /target/debug/libalpaca_autotrader_core.so \
      /tmp/compiled-extension/alpaca_autotrader_core.so
    PYTHONPATH=/tmp/compiled-extension python \
      crates/alpaca-autotrader-py/tests/compiled_bridge_parity.py \
      --binary /target/debug/alpaca-autotrader
  '
