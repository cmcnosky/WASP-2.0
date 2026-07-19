#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
container_name="alpaca-autotrader-postgres-check-$$"
postgres_image='postgres:17-alpine@sha256:742f40ea20b9ff2ff31db5458d127452988a2164df9e17441e191f3b72252193'
database_name='alpaca_autotrader_test'
database_user='test_trader'
database_password='local_ephemeral_test_only'

cleanup() {
  docker rm --force "$container_name" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

if ! command -v docker >/dev/null 2>&1; then
  printf 'PostgreSQL check requires Docker\n' >&2
  exit 1
fi

docker run --detach \
  --name "$container_name" \
  --network none \
  --tmpfs /var/lib/postgresql/data:rw,nosuid,nodev,size=512m \
  --env "POSTGRES_DB=$database_name" \
  --env "POSTGRES_USER=$database_user" \
  --env "POSTGRES_PASSWORD=$database_password" \
  "$postgres_image" >/dev/null

ready=0
for _attempt in $(seq 1 60); do
  # The official image starts a temporary postmaster while initializing and then
  # replaces PID 1 with the final postmaster. pg_isready alone can observe the
  # temporary server, so require both final PID identities before proceeding.
  if docker exec "$container_name" sh -ceu \
    'test "$(cat /proc/1/comm)" = postgres
     test "$(head -n 1 "$PGDATA/postmaster.pid")" = 1' >/dev/null 2>&1 \
    && docker exec "$container_name" pg_isready \
      --username "$database_user" --dbname "$database_name" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done

if [[ "$ready" -ne 1 ]]; then
  docker logs "$container_name" >&2 || true
  printf 'PostgreSQL did not become ready\n' >&2
  exit 1
fi

shopt -s nullglob
migration_files=("$repo_root"/migrations/*.sql)
if [[ "${#migration_files[@]}" -eq 0 ]]; then
  printf 'No migrations/*.sql files found\n' >&2
  exit 1
fi

profile_guard_database='alpaca_autotrader_profile_guard_test'
docker exec "$container_name" createdb \
  --username "$database_user" \
  "$profile_guard_database"
for migration_file in "${migration_files[@]}"; do
  if [[ "${migration_file##*/}" == "0010_json_hash_profile_v1.sql" ]]; then
    break
  fi
  docker exec --interactive "$container_name" psql \
    --username "$database_user" \
    --dbname "$profile_guard_database" \
    --set=ON_ERROR_STOP=1 \
    --file=- <"$migration_file" >/dev/null
done
docker exec "$container_name" psql \
  --username "$database_user" \
  --dbname "$profile_guard_database" \
  --set=ON_ERROR_STOP=1 \
  --command="INSERT INTO data_artifacts (artifact_id, logical_name, version, source, feed, adjustment_mode, as_of, available_at, object_uri, content_hash, metadata) VALUES ('91000000-0000-0000-0000-000000000001', 'precutover', 'v0', 'test', 'test', 'raw', clock_timestamp(), clock_timestamp(), 's3://invalid/precutover', repeat('a', 64), '{}'::jsonb);" \
  >/dev/null
if docker exec --interactive "$container_name" psql \
  --username "$database_user" \
  --dbname "$profile_guard_database" \
  --set=ON_ERROR_STOP=1 \
  --file=- <"$repo_root/migrations/0010_json_hash_profile_v1.sql" \
  >/dev/null 2>&1; then
  printf 'Hash-profile migration accepted pre-existing state\n' >&2
  exit 1
fi
docker exec "$container_name" dropdb \
  --username "$database_user" \
  "$profile_guard_database"
printf 'hash-profile empty-database migration guard passed\n'

for migration_file in "${migration_files[@]}"; do
  printf 'applying %s\n' "${migration_file#"$repo_root"/}"
  docker exec --interactive "$container_name" psql \
    --username "$database_user" \
    --dbname "$database_name" \
    --set=ON_ERROR_STOP=1 \
    --file=- <"$migration_file"
done

invariant_test="$repo_root/tests/sql/ledger_invariants.sql"
if [[ ! -f "$invariant_test" ]]; then
  printf 'Required SQL invariant test is missing: tests/sql/ledger_invariants.sql\n' >&2
  exit 1
fi

printf 'running tests/sql/ledger_invariants.sql\n'
docker exec --interactive "$container_name" psql \
  --username "$database_user" \
  --dbname "$database_name" \
  --set=ON_ERROR_STOP=1 \
  --file=- <"$invariant_test"

observer_invariant_test="$repo_root/tests/sql/observer_invariants.sql"
if [[ ! -f "$observer_invariant_test" ]]; then
  printf 'Required SQL observer invariant test is missing: tests/sql/observer_invariants.sql\n' >&2
  exit 1
fi

printf 'running tests/sql/observer_invariants.sql\n'
docker exec --interactive "$container_name" psql \
  --username "$database_user" \
  --dbname "$database_name" \
  --set=ON_ERROR_STOP=1 \
  --file=- <"$observer_invariant_test"

observer_store_source="$repo_root/crates/trader-execution/src/observer_store.rs"
observer_verifier_sql="$(awk '
  $0 == "const OBSERVER_SCHEMA_AND_PRIVILEGES_SQL: &str = r#\"" {
    in_query = 1
    next
  }
  in_query && $0 == "\"#;" { exit }
  in_query { print }
' "$observer_store_source")"
if [[ -z "$observer_verifier_sql" ]]; then
  printf 'Could not extract the exact Rust observer schema verifier\n' >&2
  exit 1
fi

observer_login='check_observer_paper'
docker exec "$container_name" psql \
  --username "$database_user" \
  --dbname "$database_name" \
  --set=ON_ERROR_STOP=1 \
  --command="CREATE ROLE $observer_login LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS; GRANT alpaca_trader_observer TO $observer_login;" \
  >/dev/null

printf 'running exact Rust observer session verifier\n'
observer_verifier_output="$(printf '%s\n' "$observer_verifier_sql" | docker exec --interactive "$container_name" psql \
  --username "$observer_login" \
  --dbname "$database_name" \
  --set=ON_ERROR_STOP=1 \
  --tuples-only \
  --no-align \
  --field-separator='|' \
  --file=-)"
observer_verifier_failures="$(printf '%s\n' "$observer_verifier_output" | awk -F '|' '
  NF != 2 || $2 != "t" { failures += 1 }
  END { print failures + 0 }
')"
if [[ -z "$observer_verifier_output" || "$observer_verifier_failures" -ne 0 ]]; then
  printf 'Exact Rust observer session verifier rejected the migrated schema\n' >&2
  printf '%s\n' "$observer_verifier_output" >&2
  exit 1
fi

docker exec "$container_name" psql \
  --username "$database_user" \
  --dbname "$database_name" \
  --set=ON_ERROR_STOP=1 \
  --command="REVOKE alpaca_trader_observer FROM $observer_login; DROP ROLE $observer_login;" \
  >/dev/null

concurrency_setup="$repo_root/tests/sql/concurrency_setup.sql"
concurrency_assertions="$repo_root/tests/sql/concurrency_assertions.sql"
if [[ ! -f "$concurrency_setup" || ! -f "$concurrency_assertions" ]]; then
  printf 'Required SQL concurrency tests are missing\n' >&2
  exit 1
fi

printf 'running PostgreSQL serialization races\n'
docker exec --interactive "$container_name" psql \
  --username "$database_user" \
  --dbname "$database_name" \
  --set=ON_ERROR_STOP=1 \
  --file=- <"$concurrency_setup"

wait_for_database_sleep() {
  local application_name="$1"
  local sleeping_sessions
  for _attempt in $(seq 1 50); do
    sleeping_sessions="$(docker exec "$container_name" psql \
      --username "$database_user" \
      --dbname "$database_name" \
      --tuples-only --no-align \
      --command="SELECT count(*) FROM pg_stat_activity WHERE application_name = '$application_name' AND wait_event_type = 'Timeout' AND wait_event = 'PgSleep';")"
    if [[ "$sleeping_sessions" == "1" ]]; then
      return 0
    fi
    sleep 0.1
  done
  return 1
}

docker exec --env PGAPPNAME=reconciliation_race_writer "$container_name" psql \
  --username "$database_user" \
  --dbname "$database_name" \
  --set=ON_ERROR_STOP=1 \
  --command="BEGIN; INSERT INTO reconciliation_diffs (reconciliation_diff_id, reconciliation_id, category, key, local_value, broker_value, resolution) VALUES ('78000000-0000-0000-0000-000000000001', '76000000-0000-0000-0000-000000000090', 'cash', 'race', '1', '2', 'unresolved'); SELECT pg_sleep(2); COMMIT;" >/dev/null &
reconciliation_writer_pid=$!

if ! wait_for_database_sleep reconciliation_race_writer; then
  wait "$reconciliation_writer_pid" || true
  printf 'Reconciliation race writer did not reach the lock-holding phase\n' >&2
  exit 1
fi

if docker exec "$container_name" psql \
  --username "$database_user" \
  --dbname "$database_name" \
  --set=ON_ERROR_STOP=1 \
  --command="UPDATE reconciliation_runs SET completed_at = clock_timestamp(), outcome = 'clean', resumable = FALSE, evidence_hash = repeat('a', 64) WHERE reconciliation_id = '76000000-0000-0000-0000-000000000090';" >/dev/null 2>&1; then
  wait "$reconciliation_writer_pid" || true
  printf 'Concurrent reconciliation completion missed a committed difference\n' >&2
  exit 1
fi

if ! wait "$reconciliation_writer_pid"; then
  printf 'Reconciliation race writer failed unexpectedly\n' >&2
  exit 1
fi

docker exec --env PGAPPNAME=fill_race_writer "$container_name" psql \
  --username "$database_user" \
  --dbname "$database_name" \
  --set=ON_ERROR_STOP=1 \
  --command="BEGIN; INSERT INTO fills (fill_id, broker_order_id, intent_id, symbol, side, quantity, price, executed_at, received_at, raw_hash) VALUES ('fill-race-1', 'broker-confirmed-test', '73000000-0000-0000-0000-000000000001', 'SPY', 'buy', 1, 500, clock_timestamp(), clock_timestamp(), repeat('b', 64)); SELECT pg_sleep(2); COMMIT;" >/dev/null &
fill_writer_pid=$!

if ! wait_for_database_sleep fill_race_writer; then
  wait "$fill_writer_pid" || true
  printf 'Fill race writer did not reach the lock-holding phase\n' >&2
  exit 1
fi

if docker exec "$container_name" psql \
  --username "$database_user" \
  --dbname "$database_name" \
  --set=ON_ERROR_STOP=1 \
  --command="INSERT INTO fills (fill_id, broker_order_id, intent_id, symbol, side, quantity, price, executed_at, received_at, raw_hash) VALUES ('fill-race-2', 'broker-confirmed-test', '73000000-0000-0000-0000-000000000001', 'SPY', 'buy', 1, 500, clock_timestamp(), clock_timestamp(), repeat('c', 64));" >/dev/null 2>&1; then
  wait "$fill_writer_pid" || true
  printf 'Concurrent fills exceeded the durable intent quantity\n' >&2
  exit 1
fi

if ! wait "$fill_writer_pid"; then
  printf 'Fill race writer failed unexpectedly\n' >&2
  exit 1
fi

docker exec --interactive "$container_name" psql \
  --username "$database_user" \
  --dbname "$database_name" \
  --set=ON_ERROR_STOP=1 \
  --file=- <"$concurrency_assertions"

docker exec "$container_name" psql \
  --username "$database_user" \
  --dbname "$database_name" \
  --tuples-only \
  --command="SELECT 'tables=' || count(*) FROM pg_tables WHERE schemaname = 'public'; SELECT 'views=' || count(*) FROM pg_views WHERE schemaname = 'public';"

printf 'PostgreSQL migrations and ledger invariants passed\n'
