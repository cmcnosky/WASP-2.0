#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

failures=0

fail() {
  printf 'clean-room: %s\n' "$1" >&2
  failures=$((failures + 1))
}

# A scanner that is missing, or that errors, must never read as "nothing found".
#
# `set -euo pipefail` does not prevent that: a command in an `if` condition is exempt from
# `set -e`, so `rg` exiting 127 (not installed) or 2 (bad pattern) landed in the same branch
# as "no matches" and all three scans below silently found nothing. The audit then printed
# that it passed. On a machine without ripgrep the legacy-identifier scan, the local-path
# scan and the credential scan were all vacuously green; CI never showed it, because GitHub
# runners ship ripgrep.
#
# The other checks here — symlinks, submodules, tracked archives, Cargo paths — use git and
# bash builtins and were unaffected, which is exactly what made this hard to notice: the
# audit did real work and reported a real result, just not the one it claimed.
require_tool() {
  command -v "$1" >/dev/null 2>&1 && return 0
  printf 'clean-room: required tool %s is not installed; refusing to report a result it did not produce\n' \
    "$1" >&2
  exit 127
}

require_tool rg
require_tool git

# rg exits 0 on a match, 1 on no match, and 2 or more on error. Only 1 means clean.
scan() {
  local status=0
  rg "$@" || status=$?
  case "$status" in
    0) return 0 ;;
    1) return 1 ;;
    *)
      printf 'clean-room: rg exited %d; the scan did not complete and its result is not trustworthy\n' \
        "$status" >&2
      exit "$status"
      ;;
  esac
}

if [[ -f .gitmodules ]]; then
  fail '.gitmodules is prohibited; use registry dependencies only'
fi

while IFS= read -r -d '' candidate_path; do
  if [[ -L "$candidate_path" ]]; then
    fail "symbolic link is prohibited: ${candidate_path#./}"
  fi
done < <(git ls-files --cached --others --exclude-standard -z)

while IFS= read -r tracked_path; do
  [[ -z "$tracked_path" ]] && continue
  case "$tracked_path" in
    *.zip|*.tar|*.tar.gz|*.tgz|*.7z|*.rar|*.db|*.sqlite|*.sqlite3|*.parquet|*.arrow|*.feather|*.pickle|*.pkl)
      fail "tracked generated/data/archive artifact is prohibited: $tracked_path"
      ;;
  esac
done < <(git ls-files --cached --others --exclude-standard)

# stinger/evidence/** is excluded for the same reason the prose files above are: this scan
# asks whether an IMPLEMENTATION SURFACE references a prior system, and those are neither.
# They are machine-generated verification logs that record the exact `docker run` the harness
# executed, absolute workdir included — so they match on this repository's own current path
# (`.../Documents/WASP 2.0/...`), not on a legacy project. Narrow on purpose: the credential
# scan at the bottom of this file still covers them, and that is the one that matters for
# anything published.
if scan --hidden --glob '!.git/**' --glob '!scripts/check-clean-room.sh' \
  --glob '!AGENTS.md' --glob '!CLEAN_ROOM.md' --glob '!README.md' \
  --glob '!docs/**' --glob '!THREAT_MODEL.md' \
  --glob '!stinger/evidence/**' \
  --line-number --ignore-case \
  '(edgeledger|documents/codex/wasp|documents/wasp([ /]|$))' .; then
  fail 'legacy project identifier found in an implementation surface'
fi

# shellcheck disable=SC2094  # reads $cargo_manifest only; nothing in this loop writes to it
while IFS= read -r cargo_manifest; do
  while IFS= read -r manifest_line; do
    if [[ "$manifest_line" =~ git[[:space:]]*= ]]; then
      fail "Git dependency is prohibited; use a reviewed registry release: $cargo_manifest"
    fi
    if [[ "$manifest_line" =~ path[[:space:]]*=[[:space:]]*\"([^\"]+)\" ]]; then
      dependency_path="${BASH_REMATCH[1]}"
      dependency_target="$(dirname "$cargo_manifest")/$dependency_path"
      if [[ ! -e "$dependency_target" ]]; then
        fail "Cargo path declaration does not exist: $cargo_manifest -> $dependency_path"
        continue
      fi
      resolved_dependency_target="$(cd "$(dirname "$dependency_target")" && pwd -P)/$(basename "$dependency_target")"
      case "$resolved_dependency_target/" in
        "$repo_root/"*) ;;
        *) fail "Cargo path declaration escapes the repository: $cargo_manifest -> $dependency_path" ;;
      esac
    fi
  done <"$cargo_manifest"
done < <(find . -path ./.git -prune -o -name Cargo.toml -type f -print)

if scan --hidden --glob '!.git/**' --glob '!scripts/check-clean-room.sh' \
  --line-number "(file://|source[[:space:]]*=[[:space:]]*[\"'](\\.\\./|/|~))" \
  --glob 'pyproject.toml' --glob 'requirements*.txt' --glob '*.tf' .; then
  fail 'external local path dependency found'
fi

if scan --hidden --glob '!.git/**' --glob '!scripts/check-clean-room.sh' \
  --line-number \
  '(AKIA[0-9A-Z]{16}|-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----|APCA-API-SECRET-KEY[[:space:]]*[:=][[:space:]]*[^$<{[:space:]])' .; then
  fail 'probable credential material found'
fi

if (( failures > 0 )); then
  printf 'clean-room audit failed with %d finding(s)\n' "$failures" >&2
  exit 1
fi

printf 'clean-room audit passed (repository-local checks only)\n'
