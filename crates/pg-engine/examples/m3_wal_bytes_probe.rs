//! M3 Stage G measurement probe (N5; tech-selection §11 R1): quantify the
//! WAL-byte cost of churn + vacuum — in particular the FullPageImage
//! amplification of compaction's batch page modifications, first
//! quantified here for `docs/phase1-m3-benchmarks.md`.
//!
//! The probe runs a fixed-row-count churn (DELETE/UPDATE/INSERT batches per
//! round, vacuum every K rounds) with a checkpoint immediately BEFORE each
//! vacuum: the checkpoint opens a fresh FPI period, so the vacuum's
//! compaction/page-free page modifications are guaranteed to be each page's
//! first touch of a period — the worst case for FPI amplification.
//!
//! It then walks the whole WAL with `WalReader` and buckets the aligned
//! on-disk bytes (`align_up(WAL_RECORD_HEADER_SIZE + payload, 8)`) by
//! record type. The byte TOTALS are split by phase (preload vs
//! churn+vacuum); the per-type buckets aggregate the WHOLE WAL (including
//! preload's checkpoint + INSERT FPIs).
//!
//! ```sh
//! cargo run -p pg-engine --release --example m3_wal_bytes_probe -- /tmp/probe_dir
//! ```
//!
//! Optional env: `PROBE_ROUNDS` (default 60), `PROBE_BATCH` (default 40),
//! `PROBE_VACUUM_EVERY` (default 5).

use std::collections::BTreeMap;
use std::path::PathBuf;

use pg_engine::{Engine, EngineConfig};
use pg_storage::types::WAL_SEGMENT_SIZE;
use pg_storage::wal::reader::WalReader;
use pg_storage::wal::record::WAL_RECORD_HEADER_SIZE;

const PAD: usize = 100;
const LIVE_ROWS: i32 = 400;

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn wal_bytes(engine: &Engine) -> u64 {
    // Segment FILES are preallocated to 16 MiB, so file length says nothing;
    // the writer's current LSN is the true end-of-WAL byte offset.
    engine.storage().wal_writer().current_lsn().0
}

fn main() {
    let dir = PathBuf::from(
        std::env::args()
            .nth(1)
            .expect("usage: m3_wal_bytes_probe <fresh-data-dir>"),
    );
    assert!(!dir.exists(), "probe dir must not exist: {}", dir.display());

    let rounds = env_u32("PROBE_ROUNDS", 60);
    let batch = env_u32("PROBE_BATCH", 40);
    let vacuum_every = env_u32("PROBE_VACUUM_EVERY", 5);

    let engine = Engine::open(&dir, EngineConfig::new(&dir)).unwrap();
    engine
        .exec(None, "CREATE TABLE t (k INT, v INT, pad TEXT)")
        .unwrap();
    engine.create_index("t", "k").unwrap();

    let mut next_key = 0i32;
    let mut live: Vec<i32> = Vec::new();
    // Preload the fixed live set in one txn.
    let txn = engine.begin_txn().unwrap();
    for _ in 0..LIVE_ROWS {
        let k = next_key;
        next_key += 1;
        engine
            .exec(
                Some(&txn),
                &format!("INSERT INTO t VALUES ({k}, {k}, '{}')", "x".repeat(PAD)),
            )
            .unwrap();
        live.push(k);
    }
    txn.commit().unwrap();
    engine.checkpoint().unwrap();
    let wal_after_preload = wal_bytes(&engine);

    let mut stats_total = pg_engine::VacuumStats::default();
    let mut vacuums = 0u32;
    for round in 0..rounds {
        // DELETE the oldest slice.
        let txn = engine.begin_txn().unwrap();
        let victims: Vec<i32> = live.drain(..(batch as usize).min(live.len())).collect();
        for k in &victims {
            engine
                .exec(Some(&txn), &format!("DELETE FROM t WHERE k = {k}"))
                .unwrap();
        }
        txn.commit().unwrap();
        // UPDATE every 7th survivor (unindexed column => HOT when it fits).
        let txn = engine.begin_txn().unwrap();
        let targets: Vec<i32> = live
            .iter()
            .skip(3)
            .step_by(7)
            .take(batch as usize)
            .copied()
            .collect();
        for k in targets {
            let v = 1_000_000 + round as i32;
            engine
                .exec(Some(&txn), &format!("UPDATE t SET v = {v} WHERE k = {k}"))
                .unwrap();
        }
        txn.commit().unwrap();
        // INSERT fresh keys.
        let txn = engine.begin_txn().unwrap();
        for _ in 0..batch {
            let k = next_key;
            next_key += 1;
            engine
                .exec(
                    Some(&txn),
                    &format!("INSERT INTO t VALUES ({k}, {k}, '{}')", "x".repeat(PAD)),
                )
                .unwrap();
            live.push(k);
        }
        txn.commit().unwrap();

        if (round + 1) % vacuum_every == 0 {
            // Fresh FPI period right before vacuum: worst case for the
            // compaction FPI amplification being measured. Set
            // `PROBE_PRE_VACUUM_CHECKPOINT=0` for the contrasting run
            // (vacuum inside a long-lived FPI period: each page's FPI is
            // amortized over the whole checkpoint interval).
            if env_u32("PROBE_PRE_VACUUM_CHECKPOINT", 1) == 1 {
                engine.checkpoint().unwrap();
            }
            let s = engine.vacuum("t").unwrap();
            vacuums += 1;
            stats_total.dead_tuples += s.dead_tuples;
            stats_total.index_keys += s.index_keys;
            stats_total.index_entries_removed += s.index_entries_removed;
            stats_total.index_entries_already_gone += s.index_entries_already_gone;
        }
    }
    engine.checkpoint().unwrap();
    let wal_final = wal_bytes(&engine);
    engine.shutdown();

    // Walk the WAL, bucketing aligned on-disk bytes by record type.
    let mut by_type: BTreeMap<String, (u64, u64)> = BTreeMap::new(); // (count, bytes)
    let mut reader = WalReader::open(dir.join("wal"), WAL_SEGMENT_SIZE).unwrap();
    let mut records = reader.records();
    while let Some(rec) = records.next().transpose().unwrap() {
        let aligned = (WAL_RECORD_HEADER_SIZE + rec.payload.len() + 7) as u64 & !7;
        let entry = by_type.entry(format!("{:?}", rec.record_type)).or_default();
        entry.0 += 1;
        entry.1 += aligned;
    }

    println!("probe dir: {}", dir.display());
    println!("rounds={rounds} batch={batch} vacuum_every={vacuum_every} vacuums={vacuums}");
    println!("vacuum stats total: {stats_total:?}");
    println!(
        "WAL bytes: preload={wal_after_preload} churn+vacuum={} total={wal_final}",
        wal_final - wal_after_preload
    );
    println!(
        "{:>24} {:>10} {:>14}",
        "record type", "count", "bytes(aligned)"
    );
    let mut total_bytes = 0u64;
    for (ty, (count, bytes)) in &by_type {
        println!("{ty:>24} {count:>10} {bytes:>14}");
        total_bytes += bytes;
    }
    println!("{:>24} {:>10} {total_bytes:>14}", "TOTAL", "");
    if let Some((fpi_count, fpi_bytes)) = by_type.get("FullPageImage") {
        println!(
            "FPI share: {fpi_count} records, {fpi_bytes} bytes = {:.1}% of total",
            *fpi_bytes as f64 * 100.0 / total_bytes as f64
        );
    }
}
