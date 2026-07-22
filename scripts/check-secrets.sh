#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# A scanner that is missing, or that errors, must never read as "nothing found".
#
# `set -euo pipefail` does not prevent that on its own: a command in an `if` condition is
# exempt from `set -e`, so `rg` exiting 127 (not installed) or 2 (bad pattern) landed in the
# same branch as "no matches", and this script went on to print "secret scan passed" having
# scanned nothing at all. On a machine without ripgrep — a fresh clone on macOS, say — the
# gate was vacuously green. CI never showed it, because GitHub runners ship ripgrep.
#
# This repository's claim is that its gates fail closed. A gate that reports green having
# checked nothing is the one defect it cannot afford, so the tool is required up front and a
# scanner error is fatal rather than clean.
require_tool() {
  command -v "$1" >/dev/null 2>&1 && return 0
  printf '%s: required tool %s is not installed; refusing to report a result it did not produce\n' \
    "${0##*/}" "$1" >&2
  exit 127
}

require_tool rg

# rg exits 0 on a match, 1 on no match, and 2 or more on error. Only 1 means clean.
scan() {
  local status=0
  rg "$@" || status=$?
  case "$status" in
    0) return 0 ;;
    1) return 1 ;;
    *)
      printf '%s: rg exited %d; the scan did not complete and its result is not trustworthy\n' \
        "${0##*/}" "$status" >&2
      exit "$status"
      ;;
  esac
}

patterns='(AKIA[0-9A-Z]{16}|ASIA[0-9A-Z]{16}|-----BEGIN (RSA |DSA |EC |OPENSSH )?PRIVATE KEY-----|xox[baprs]-[0-9A-Za-z-]{10,}|gh[pousr]_[0-9A-Za-z]{20,})'

if scan --hidden --glob '!.git/**' --glob '!scripts/check-secrets.sh' \
  --line-number "$patterns" .; then
  printf 'secret scan failed: probable credential material found\n' >&2
  exit 1
fi

printf 'secret scan passed\n'
