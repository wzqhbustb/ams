#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
dsn="${PG_RUST_DSN:-host=127.0.0.1 port=55432 user=pg_rust dbname=pg_rust}"
out="${PG_RUST_TEST_OUT:-target/tmp/stagec-psql-$$.out}"
mkdir -p target/tmp
trap 'rm -f "$out"' EXIT

psql -qAt "$dsn" -f scripts/phase4a_stagec_psql.sql > "$out"
diff -u scripts/phase4a_stagec_psql.expected "$out"
echo "psql Stage C acceptance: PASS"
