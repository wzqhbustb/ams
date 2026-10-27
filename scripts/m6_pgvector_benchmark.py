#!/usr/bin/env python3
"""M6 Stage F: the pgvector side of the siftsmall benchmark (coding-plan
行 300) — same dataset, same queries, same recall/latency/throughput
metrics as `crates/pg-engine/tests/m6_benchmark.rs`.

Prereqs (already satisfied on this machine, see docs/phase2-m6-benchmarks.md):
  brew install postgresql pgvector

Everything lives under `target/pgvector-harness/` (no writes outside the
workspace). The brew formula installs the extension into postgres's own
directories, so `CREATE EXTENSION vector` works without custom paths.

Usage: python3 scripts/m6_pgvector_benchmark.py
"""

import os
import re
import struct
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "datasets" / "siftsmall"
WORK = ROOT / "target" / "pgvector-harness"
PGDATA = WORK / "pgdata"
SOCKET = WORK / "socket"
LOG = WORK / "postgres.log"


def pg_bin() -> Path:
    # pgvector's brew dependency is postgresql@17 — use its binaries so the
    # extension's ABI matches the server exactly.
    prefix = os.environ.get("PG_PREFIX")
    if prefix is None:
        prefix = subprocess.run(
            ["brew", "--prefix", "postgresql@17"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    return Path(prefix) / "bin"


def run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, check=True, **kw)


def psql(bin_dir: Path, db: str, sql: str) -> str:
    return run([str(bin_dir / "psql"), "-h", str(SOCKET), "-d", db, "-X", "-A", "-t", "-c", sql]).stdout


def psql_file(bin_dir: Path, db: str, path: Path) -> str:
    return run([str(bin_dir / "psql"), "-h", str(SOCKET), "-d", db, "-X", "-A", "-t", "-f", str(path)]).stdout


def read_fvecs(path: Path):
    out = []
    with open(path, "rb") as f:
        while True:
            dim_bytes = f.read(4)
            if len(dim_bytes) != 4:
                break
            dim = struct.unpack("<i", dim_bytes)[0]
            vals = struct.unpack(f"<{dim}f", f.read(4 * dim))
            out.append(vals)
    return out


def read_ivecs(path: Path):
    out = []
    with open(path, "rb") as f:
        while True:
            dim_bytes = f.read(4)
            if len(dim_bytes) != 4:
                break
            dim = struct.unpack("<i", dim_bytes)[0]
            vals = struct.unpack(f"<{dim}i", f.read(4 * dim))
            out.append(vals)
    return out


def vec_text(v):
    return "[" + ", ".join(str(x) for x in v) + "]"


def main():
    base = read_fvecs(DATA / "siftsmall_base.fvecs")
    queries = read_fvecs(DATA / "siftsmall_query.fvecs")
    gt = read_ivecs(DATA / "siftsmall_groundtruth.ivecs")
    assert len(base) == 10_000 and len(queries) == 100 and len(gt) == 100
    bin_dir = pg_bin()
    WORK.mkdir(exist_ok=True)
    SOCKET.mkdir(exist_ok=True)
    if not (PGDATA / "PG_VERSION").exists():
        run([str(bin_dir / "initdb"), "-D", str(PGDATA), "--auth=trust", "--no-locale"])

    # Start (idempotent): stop any stale server first.
    subprocess.run(
        [str(bin_dir / "pg_ctl"), "-D", str(PGDATA), "stop", "-m", "immediate"],
        capture_output=True,
    )
    run(
        [
            str(bin_dir / "pg_ctl"),
            "-D",
            str(PGDATA),
            "-o",
            f"-c unix_socket_directories={SOCKET} -c listen_addresses=",
            "-l",
            str(LOG),
            "start",
        ]
    )
    try:
        # Wait for readiness.
        for _ in range(100):
            try:
                psql(bin_dir, "postgres", "SELECT 1")
                break
            except subprocess.CalledProcessError:
                time.sleep(0.1)
        psql(bin_dir, "postgres", "CREATE EXTENSION IF NOT EXISTS vector")
        psql(bin_dir, "postgres", "DROP TABLE IF EXISTS items")
        psql(bin_dir, "postgres", "CREATE TABLE items (id int, embedding vector(128))")

        # Bulk load (10k rows in one transaction).
        sql_path = WORK / "load.sql"
        with open(sql_path, "w") as f:
            f.write("BEGIN;\n")
            for i, v in enumerate(base):
                f.write(f"INSERT INTO items VALUES ({i}, '{vec_text(v)}');\n")
            f.write("COMMIT;\n")
        started = time.monotonic()
        psql_file(bin_dir, "postgres", sql_path)
        bulk_elapsed = time.monotonic() - started
        print(
            f"PGV-BENCH bulk_insert: {len(base)} rows in {bulk_elapsed:.2f}s "
            f"= {len(base) / bulk_elapsed:.0f} rows/s (single txn)",
            flush=True,
        )

        # HNSW index build (m=16, ef_construction=200 — parity with ours).
        started = time.monotonic()
        psql(
            bin_dir,
            "postgres",
            "CREATE INDEX items_embedding_idx ON items USING hnsw (embedding vector_l2_ops) WITH (m = 16, ef_construction = 200)",
        )
        index_elapsed = time.monotonic() - started
        print(f"PGV-BENCH hnsw_index_build: {index_elapsed:.2f}s", flush=True)

        # Per-statement insert throughput (100 auto-commit inserts in ONE
        # psql session, \timing per statement — no process-start overhead).
        auto_sql = WORK / "auto_insert.sql"
        with open(auto_sql, "w") as f:
            f.write("\\timing on\n")
            for i in range(100):
                f.write(f"INSERT INTO items VALUES ({i}, '{vec_text(base[i])}');\n")
        started = time.monotonic()
        auto_out = psql_file(bin_dir, "postgres", auto_sql)
        auto_elapsed = time.monotonic() - started
        stmt_times = [
            float(m.group(1)) for m in re.finditer(r"Time: ([0-9.]+) ms", auto_out)
        ]
        print(
            f"PGV-BENCH auto_commit_insert: 100 rows in {auto_elapsed:.2f}s "
            f"= {100 / auto_elapsed:.0f} rows/s (per-statement, one session; "
            f"server-side sum {sum(stmt_times):.1f}ms)",
            flush=True,
        )

        # ANN queries: recall vs ground truth + per-statement latency
        # (\timing on; EXPLAIN alone returns no rows).
        # One-time proof that the GUC really takes effect in a session
        # (kept OUT of the per-query files so it can't pollute id parsing).
        check_sql = WORK / "check-ef.sql"
        with open(check_sql, "w") as f:
            f.write("LOAD 'vector';\nSET hnsw.ef_search = 64;\nSHOW hnsw.ef_search;\n")
        ef_out = psql_file(bin_dir, "postgres", check_sql)
        assert "64" in ef_out, f"ef_search=64 did not take effect: {ef_out}"
        overlaps = []
        latencies = []
        for qi, q in enumerate(queries):
            sql_path = WORK / f"query-{qi}.sql"
            with open(sql_path, "w") as f:
                # GUC is per-session (review P3-1): every query file must
                # LOAD the extension and SET ef_search in ITS OWN session —
                # a SET in another psql process has no effect here (and an
                # unloaded session would even reject the parameter).
                f.write("LOAD 'vector';\n")
                f.write("SET hnsw.ef_search = 64;\n")
                f.write("\\timing on\n")
                f.write(f"SELECT id FROM items ORDER BY embedding <-> '{vec_text(q)}' LIMIT 10;\n")
            out = psql_file(bin_dir, "postgres", sql_path)
            ids = [int(m.group(1)) for m in re.finditer(r"^\s*(\d+)\s*$", out, re.M)]
            m = re.search(r"Time: ([0-9.]+) ms", out)
            if not m:
                raise RuntimeError(f"cannot parse execution time for query {qi}: {out}")
            latencies.append(float(m.group(1)))
            truth = set(gt[qi][:10])
            overlaps.append(len(truth & set(ids)) / len(truth))
        recall = sum(overlaps) / len(overlaps)
        latencies.sort()
        mean = sum(latencies) / len(latencies)
        p50 = latencies[len(latencies) // 2]
        p95 = latencies[int(len(latencies) * 0.95)]
        print(
            f"PGV-BENCH query: recall@10={recall:.4f}; latency mean={mean:.3f}ms "
            f"p50={p50:.3f}ms p95={p95:.3f}ms max={latencies[-1]:.3f}ms (ef=64)",
            flush=True,
        )
    finally:
        subprocess.run(
            [str(bin_dir / "pg_ctl"), "-D", str(PGDATA), "stop", "-m", "immediate"],
            capture_output=True,
        )


if __name__ == "__main__":
    sys.exit(main())
