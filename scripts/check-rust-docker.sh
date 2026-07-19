#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
rust_image='rust:1.88.0-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0'

if ! command -v docker >/dev/null 2>&1; then
  printf 'Rust checks unavailable: neither cargo nor docker is installed\n' >&2
  exit 1
fi

docker run --rm \
  --cap-drop=ALL \
  --security-opt=no-new-privileges:true \
  --tmpfs /tmp:rw,nosuid,nodev,size=64m \
  --tmpfs /cargo-home:rw,exec,nosuid,nodev,size=1g \
  --mount "type=bind,src=${repo_root},dst=/workspace,readonly" \
  --mount type=volume,dst=/workspace/target \
  --env CARGO_HOME=/cargo-home \
  --env CARGO_TARGET_DIR=/workspace/target \
  --workdir /workspace \
  "$rust_image" \
  bash -c 'cargo fmt --all --check && cargo clippy --workspace --all-targets --all-features --locked -- -D warnings && cargo test --workspace --all-features --locked'
