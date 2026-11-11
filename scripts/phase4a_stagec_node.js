#!/usr/bin/env node
/* Phase 4a Stage C node-postgres acceptance probe.
 * Run with NODE_PATH=target/tmp/stagec-node/node_modules and PG_RUST_DSN
 * (default host=127.0.0.1 port=55432 user=pg_rust database=pg_rust).
 */

const { Client } = require("pg");

async function main() {
  const client = process.env.PG_RUST_DSN
    ? new Client({ connectionString: process.env.PG_RUST_DSN })
    : new Client({
        host: "127.0.0.1",
        port: 55432,
        user: "pg_rust",
        database: "pg_rust",
      });
  await client.connect();

  await client.query("CREATE TABLE stagec_node (id INT, big BIGINT, score FLOAT8, name TEXT)");
  await client.query({
    text: "INSERT INTO stagec_node VALUES ($1, $2, $3, $4), ($5, $6, $7, $8)",
    values: [1, 10, 1.25, "one", 2, 20, -2.5, null],
  });
  const selected = await client.query("SELECT id, big, score, name FROM stagec_node ORDER BY id");
  if (selected.rows.length !== 2 || selected.rows[0].id !== 1 || selected.rows[1].name !== null) {
    throw new Error(`unexpected selected rows: ${JSON.stringify(selected.rows)}`);
  }

  await client.query({ text: "UPDATE stagec_node SET name = $1 WHERE id = $2", values: ["updated", 1] });
  await client.query({ text: "DELETE FROM stagec_node WHERE id = $1", values: [2] });
  const updated = await client.query({ text: "SELECT name FROM stagec_node WHERE id = $1", values: [1] });
  if (updated.rows[0].name !== "updated") throw new Error("parameterized UPDATE failed");

  await client.query("BEGIN");
  await client.query({
    text: "INSERT INTO stagec_node VALUES ($1, $2, $3, $4)",
    values: [3, 30, 0.0, "rollback"],
  });
  await client.query("ROLLBACK");
  const afterRollback = await client.query("SELECT id FROM stagec_node");
  if (afterRollback.rows.length !== 1 || afterRollback.rows[0].id !== 1) {
    throw new Error("ROLLBACK did not preserve exactly the original row");
  }

  await client.query("CREATE TABLE stagec_node_ann (id INT, v VECTOR(2))");
  await client.query("CREATE INDEX stagec_node_ann_idx ON stagec_node_ann USING hnsw (v vector_l2_ops)");
  await client.query({
    text: "INSERT INTO stagec_node_ann VALUES ($1, $2), ($3, $4)",
    values: [1, "[1,0]", 2, "[0,1]"],
  });
  const ann = await client.query({
    text: "SELECT id FROM stagec_node_ann ORDER BY v <-> $1 ASC LIMIT 1",
    values: ["[1,0]"],
  });
  if (ann.rows[0].id !== 1) throw new Error("parameterized ANN failed");

  await client.end();
  console.log("node-postgres Stage C acceptance: PASS");
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
