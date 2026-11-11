#!/usr/bin/env python3
"""Phase 4a Stage C psycopg2 acceptance probe.

Run against a fresh pg-server data directory:
  PG_RUST_DSN="host=127.0.0.1 port=55432 user=pg_rust dbname=pg_rust" \
    target/tmp/stagec-drivers-venv/bin/python scripts/phase4a_stagec_psycopg2.py
"""

import os

import psycopg2
from psycopg2.extensions import AsIs


def main() -> None:
    dsn = os.environ.get(
        "PG_RUST_DSN", "host=127.0.0.1 port=55432 user=pg_rust dbname=pg_rust"
    )
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("CREATE TABLE stagec_py (id INT, big BIGINT, score FLOAT8, name TEXT)")
    cur.execute(
        "INSERT INTO stagec_py VALUES (%s, %s, %s, %s), (%s, %s, %s, %s)",
        (1, 10, 1.25, "one", 2, 20, -2.5, None),
    )
    cur.execute("SELECT id, big, score, name FROM stagec_py ORDER BY id")
    rows = cur.fetchall()
    assert rows == [(1, 10, 1.25, "one"), (2, 20, -2.5, None)], rows

    cur.execute("UPDATE stagec_py SET name = %s WHERE id = %s", ("updated", 1))
    cur.execute("DELETE FROM stagec_py WHERE id = %s", (2,))
    cur.execute("SELECT name FROM stagec_py WHERE id = %s", (1,))
    assert cur.fetchone()[0] == "updated"

    cur.execute("BEGIN")
    cur.execute("INSERT INTO stagec_py VALUES (%s, %s, %s, %s)", (3, 30, 0.0, "rollback"))
    cur.execute("ROLLBACK")
    cur.execute("SELECT id FROM stagec_py")
    assert cur.fetchall() == [(1,)]

    cur.execute("CREATE TABLE stagec_py_ann (id INT, v VECTOR(2))")
    cur.execute("CREATE INDEX stagec_py_ann_idx ON stagec_py_ann USING hnsw (v vector_l2_ops)")
    # psycopg2 composes parameters client-side. SQL objects emit the raw
    # pgvector array literal instead of a quoted TEXT literal.
    cur.execute(
        "INSERT INTO stagec_py_ann VALUES (%s, %s), (%s, %s)",
        (1, AsIs("[1,0]"), 2, AsIs("[0,1]")),
    )
    cur.execute("SELECT id FROM stagec_py_ann ORDER BY v <-> %s ASC LIMIT 1", (AsIs("[1,0]"),))
    assert cur.fetchone()[0] == 1

    cur.close()
    conn.close()
    print("psycopg2 Stage C acceptance: PASS")


if __name__ == "__main__":
    main()
