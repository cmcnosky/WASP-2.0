#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

failures=0

fail() {
  printf 'clean-room: %s\n' "$1" >&2
  failures=$((failures + 1))
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

if rg --hidden --glob '!.git/**' --glob '!scripts/check-clean-room.sh' \
  --glob '!AGENTS.md' --glob '!CLEAN_ROOM.md' --glob '!README.md' \
  --glob '!docs/**' --glob '!THREAT_MODEL.md' \
  --line-number --ignore-case \
  '(edgeledger|documents/codex/wasp|documents/wasp([ /]|$))' .; then
  fail 'legacy project identifier found in an implementation surface'
fi

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

if rg --hidden --glob '!.git/**' --glob '!scripts/check-clean-room.sh' \
  --line-number "(file://|source[[:space:]]*=[[:space:]]*[\"'](\\.\\./|/|~))" \
  --glob 'pyproject.toml' --glob 'requirements*.txt' --glob '*.tf' .; then
  fail 'external local path dependency found'
fi

if rg --hidden --glob '!.git/**' --glob '!scripts/check-clean-room.sh' \
  --line-number \
  '(AKIA[0-9A-Z]{16}|-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----|APCA-API-SECRET-KEY[[:space:]]*[:=][[:space:]]*[^$<{[:space:]])' .; then
  fail 'probable credential material found'
fi

if (( failures > 0 )); then
  printf 'clean-room audit failed with %d finding(s)\n' "$failures" >&2
  exit 1
fi

printf 'clean-room audit passed (repository-local checks only)\n'
