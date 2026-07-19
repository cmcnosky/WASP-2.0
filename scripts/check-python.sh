#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"

if command -v python3 >/dev/null 2>&1 && \
  python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)'; then
  PYTHONDONTWRITEBYTECODE=1 PYTHONPATH="$repo_root/python/src" \
    python3 -m unittest discover -s "$repo_root/python/tests"
  exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
  printf 'Python tests require Python 3.12+ or Docker\n' >&2
  exit 1
fi

python_image='python:3.12.10-slim-bookworm@sha256:fd95fa221297a88e1cf49c55ec1828edd7c5a428187e67b5d1805692d11588db'

docker run --rm \
  --read-only \
  --network none \
  --cap-drop=ALL \
  --security-opt=no-new-privileges:true \
  --tmpfs /tmp:rw,nosuid,nodev,size=64m \
  --mount "type=bind,src=${repo_root},dst=/workspace,readonly" \
  --env PYTHONDONTWRITEBYTECODE=1 \
  --env PYTHONPATH=/workspace/python/src \
  --workdir /workspace \
  "$python_image" \
  python -m unittest discover -s python/tests
