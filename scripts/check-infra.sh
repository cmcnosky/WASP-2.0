#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
terraform_root="$repo_root/infra/terraform"

if command -v terraform >/dev/null 2>&1; then
  terraform_data_dir="$(mktemp -d)"
  export TF_DATA_DIR="$terraform_data_dir"
  terraform -chdir="$terraform_root" fmt -check -recursive
  terraform -chdir="$terraform_root" init -backend=false -input=false -lockfile=readonly
  terraform -chdir="$terraform_root" validate
  terraform -chdir="$terraform_root" test -test-directory=tests
  printf 'terraform formatting and validation passed\n'
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  printf 'Infrastructure check requires Terraform or Docker\n' >&2
  exit 1
fi

terraform_image='hashicorp/terraform:1.8.5@sha256:c2de8d7f1919b8e534c4e7cb92c2b327baafd87010d5a3bba036da05caa12db0'

docker run --rm \
  --read-only \
  --cap-drop=ALL \
  --security-opt=no-new-privileges:true \
  --tmpfs /tmp:rw,exec,nosuid,nodev,size=2g \
  --mount "type=bind,src=${repo_root},dst=/workspace,readonly" \
  --env TF_DATA_DIR=/tmp/terraform-data \
  --env TF_PLUGIN_CACHE_DIR=/tmp/plugin-cache \
  --workdir /workspace/infra/terraform \
  --entrypoint /bin/sh \
  "$terraform_image" \
  -c 'mkdir -p "$TF_DATA_DIR" "$TF_PLUGIN_CACHE_DIR" && terraform fmt -check -recursive && terraform init -backend=false -input=false -lockfile=readonly && terraform validate && terraform test -test-directory=tests'

printf 'terraform formatting and validation passed in pinned container\n'
