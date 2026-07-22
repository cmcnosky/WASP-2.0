#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
provided_image="${1:-}"
image_ref="$provided_image"
built_image=false

if ! command -v docker >/dev/null 2>&1; then
  printf 'Container contract check requires Docker\n' >&2
  exit 1
fi

cleanup() {
  if [[ "$built_image" == true ]]; then
    docker image rm --force "$image_ref" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ -z "$image_ref" ]]; then
  image_ref="wasp2-container-contract:${PPID}-${RANDOM}"
  built_image=true
  docker build \
    --build-arg APP_PACKAGE=alpaca-autotrader \
    --tag "$image_ref" \
    "$repo_root"
fi

entrypoint="$(docker image inspect --format '{{json .Config.Entrypoint}}' "$image_ref")"
container_user="$(docker image inspect --format '{{.Config.User}}' "$image_ref")"
if [[ "$entrypoint" != '["/app/alpaca-autotrader"]' ]]; then
  printf 'Container entrypoint is not the reviewed application binary: %s\n' "$entrypoint" >&2
  exit 1
fi
if [[ "$container_user" != 'nonroot:nonroot' && "$container_user" != '65532:65532' ]]; then
  printf 'Container user is not the reviewed nonroot identity: %s\n' "$container_user" >&2
  exit 1
fi

# shellcheck disable=SC2054  # elements are newline-separated; the commas are tmpfs mount
#                              options inside a single element, not array separators
runtime_args=(
  --rm
  --network=none
  --read-only
  --cap-drop=ALL
  --security-opt=no-new-privileges:true
  --pids-limit=64
  --memory=256m
  --cpus=1
  --tmpfs=/tmp:rw,nosuid,nodev,size=16m
)

health_output="$(docker run "${runtime_args[@]}" "$image_ref" health --local)"
HEALTH_OUTPUT="$health_output" python3 - <<'PY'
import json
import os

record = json.loads(os.environ["HEALTH_OUTPUT"])
expected = {
    "mode": "local",
    "provider_access": False,
    "status": "ok",
    "submission_enabled": False,
}
for key, value in expected.items():
    if record.get(key) != value:
        raise SystemExit(f"container local-health contract mismatch for {key}")
if not isinstance(record.get("version"), str) or not record["version"]:
    raise SystemExit("container local-health contract omitted its version")
PY

set +e
observer_output="$(docker run "${runtime_args[@]}" "$image_ref" paper-observer 2>&1)"
observer_status=$?
set -e
if [[ "$observer_status" -eq 0 ]]; then
  printf 'Paper observer accepted an empty environment\n' >&2
  exit 1
fi
if [[ "$observer_output" != *'paper observer configuration was rejected'* ]]; then
  printf 'Paper observer did not fail with the fixed redacted configuration error\n' >&2
  exit 1
fi
for forbidden in ALPACA_API_KEY_ID ALPACA_API_SECRET_KEY OBSERVER_DATABASE_PASSWORD account_fingerprint_salt_hex; do
  if [[ "$observer_output" == *"$forbidden"* ]]; then
    printf 'Paper observer configuration failure disclosed a sensitive input name\n' >&2
    exit 1
  fi
done

printf 'container command and fail-closed observer contract passed\n'
