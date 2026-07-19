#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

./scripts/check-clean-room.sh
./scripts/check-secrets.sh

for shell_script in scripts/*.sh; do
  bash -n "$shell_script"
done

if command -v shellcheck >/dev/null 2>&1; then
  shellcheck scripts/*.sh
else
  printf 'shellcheck skipped: command is not installed\n'
fi

if [[ -f Cargo.toml ]] && command -v cargo >/dev/null 2>&1; then
  cargo fmt --all --check
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  cargo test --workspace --all-features --locked
elif [[ -f Cargo.toml ]]; then
  ./scripts/check-rust-docker.sh
else
  printf 'Rust checks skipped: Cargo.toml is unavailable\n'
fi

if [[ -d python/tests ]]; then
  ./scripts/check-python.sh
  if command -v ruff >/dev/null 2>&1; then
    ruff check python
  else
    printf 'ruff skipped: command is not installed\n'
  fi
  if command -v mypy >/dev/null 2>&1; then
    mypy python/src python/tests
  else
    printf 'mypy skipped: command is not installed\n'
  fi
fi

./scripts/check-pyo3.sh

if command -v docker >/dev/null 2>&1; then
  ./scripts/check-container-contract.sh
else
  printf 'Container contract check skipped: docker is not installed\n'
fi

./scripts/check-infra.sh

if command -v docker >/dev/null 2>&1; then
  docker compose config --quiet
  ./scripts/check-postgres.sh
else
  printf 'Compose and PostgreSQL checks skipped: docker is not installed\n'
fi

printf 'repository checks completed\n'
