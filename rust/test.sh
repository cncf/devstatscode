#!/usr/bin/env bash
# Run every kind of test we have for the Rust port (Linux and FreeBSD):
#   1. rustfmt --check                      (formatting)
#   2. cargo clippy -D warnings             (lints, all targets)
#   3. cargo test --workspace               (library unit tests, binary unit tests,
#                                            doc tests, and the Go⇄Rust compatibility
#                                            tests in cmd/<name>/tests/compat.rs which
#                                            build the Go binaries from ../cmd/<name>)
# Artifacts go to target/<os>/ (see env.sh), so VM and host runs do not collide.
#
# Usage: ./test.sh [--skip-go] [--skip-db] [--no-lint] [--lint-only] [--release] [-- <cargo test args>]
#   --skip-go    do not build/compare against the Go binaries (DEVSTATS_SKIP_GO_COMPAT=1)
#   --skip-db    do not run the PostgreSQL-backed tests (DEVSTATS_SKIP_DB_TESTS=1)
#   --no-lint    skip rustfmt/clippy
#   --lint-only  run only rustfmt/clippy
#   --release    run tests with the release profile
#   --           everything after is passed to `cargo test` (e.g. `-- -p replacer compat`);
#                when it selects packages (`-p`/`--package`) only those are tested,
#                otherwise the whole workspace
# Env:
#   DEVSTATS_GO  path to the go tool if it is not on PATH
#   PG_HOST/PG_PORT/PG_USER/PG_PASS
#                PostgreSQL server for the DB-backed tests. Defaults: 127.0.0.1,
#                the first of ports 5432 (host) / 15432 (VM → host ssh tunnel)
#                that accepts connections, gha_admin/password. PG_DB is forced
#                to "dbtest" (the tests create/drop `dbtest_*` databases).
#                When no server is reachable the DB tests are skipped (a note is
#                printed) — the rest of the suite still runs.
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source=./env.sh
. ./env.sh

lint=1
tests=1
release=()
extra=()
while [ $# -gt 0 ]; do
  case "$1" in
    --skip-go) export DEVSTATS_SKIP_GO_COMPAT=1 ;;
    --skip-db) export DEVSTATS_SKIP_DB_TESTS=1 ;;
    --no-lint) lint=0 ;;
    --lint-only) tests=0 ;;
    --release) release=(--release) ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    --) shift; extra=("$@"); break ;;
    *) echo "unknown option: $1" >&2; exit 1 ;;
  esac
  shift
done

step() { echo; echo "==> $*"; }

if [ "$lint" = 1 ]; then
  step "rustfmt --check"
  cargo fmt --all -- --check
  step "clippy -D warnings"
  cargo clippy --workspace --all-targets "${release[@]}" -- -D warnings
fi

# True when something listens on $1:$2 (bash /dev/tcp, no nc dependency).
port_open() {
  (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null
}

# Locate the PostgreSQL server used by the DB-backed tests (see header).
setup_pg() {
  if [ "${DEVSTATS_SKIP_DB_TESTS:-}" != "" ]; then
    echo "DB tests: skipped (DEVSTATS_SKIP_DB_TESTS set)"
    return
  fi
  export PG_HOST="${PG_HOST:-127.0.0.1}"
  export PG_USER="${PG_USER:-gha_admin}"
  export PG_PASS="${PG_PASS:-password}"
  export PG_DB=dbtest
  if [ -z "${PG_PORT:-}" ]; then
    for p in 5432 15432; do
      if port_open "$PG_HOST" "$p"; then
        PG_PORT="$p"
        break
      fi
    done
  fi
  if [ -z "${PG_PORT:-}" ] || ! port_open "$PG_HOST" "$PG_PORT"; then
    echo "DB tests: skipped (no PostgreSQL at $PG_HOST:${PG_PORT:-5432/15432})"
    export DEVSTATS_SKIP_DB_TESTS=1
    unset PG_PORT
    return
  fi
  export PG_PORT
  # No log/time output from the library and no 60 s sleep before fatal exits.
  export GHA2DB_SKIPLOG=1 GHA2DB_SKIPTIME=1 NO_FATAL_DELAY=1
  echo "DB tests: PostgreSQL at $PG_HOST:$PG_PORT as $PG_USER (databases dbtest_*)"
}

if [ "$tests" = 1 ]; then
  devstats_ensure_go
  setup_pg
  # cargo treats `--workspace -p x` as "all members" — drop --workspace when packages are selected
  scope=(--workspace)
  for a in "${extra[@]}"; do
    case "$a" in
      -p|--package|-p?*|--package=*) scope=() ;;
    esac
  done
  step "cargo test ${scope[*]:-} ${release[*]:-} ${extra[*]:-} (os: $DEVSTATS_OS, target: $CARGO_TARGET_DIR)"
  cargo test "${scope[@]}" "${release[@]}" "${extra[@]}"
fi

step "OK — all checks passed"
