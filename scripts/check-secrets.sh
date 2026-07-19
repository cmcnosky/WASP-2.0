#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

patterns='(AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|-----BEGIN (RSA |DSA |EC |OPENSSH )?PRIVATE KEY-----|xox[baprs]-[0-9A-Za-z-]{10,}|gh[pousr]_[0-9A-Za-z]{20,})'

if rg --hidden --glob '!.git/**' --glob '!scripts/check-secrets.sh' \
  --line-number "$patterns" .; then
  printf 'secret scan failed: probable credential material found\n' >&2
  exit 1
fi

printf 'secret scan passed\n'
