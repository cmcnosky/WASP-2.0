#!/usr/bin/env bash
# The repository's own gate. The house rules require this to pass before hand-off.
set -euo pipefail
python -m pytest -q
