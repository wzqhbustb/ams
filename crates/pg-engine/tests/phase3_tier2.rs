use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use pg_engine::{Engine, EngineConfig, QueryResult};

fn run_benchmark(merge_enabled: bool) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = EngineConfig::new(tmp.path());
    config.inverted_merge_enabled = merge_enabled;
    let engine = Arc::new(Engine::open(tmp.path(), config).unwrap());
    engine
        .exec(None, "CREATE TABLE t (id INT4, content TEXT)")
        .unwrap();
    engine
        .exec(
            None,
            "CREATE INDEX content_idx ON t USING inverted (content)",
        )
        .unwrap();

    // Seed 4,000 documents in batches, then commit another 6,000 concurrently.
    for batch in 0..40 {
        let mut sql = "INSERT INTO t VALUES ".to_string();
        for offset in 0..100 {
            let id = batch * 100 + offset;
            let text = format!("shared unique{id} {}", "x".repeat(1000));
            sql.push_str(&format!("({id}, '{text}')"));
            if offset != 99 {
                sql.push(',');
            }
        }
        engine.exec(None, &sql).unwrap();
    }

    let barrier = Arc::new(Barrier::new(16));
    let samples = Arc::new(std::sync::Mutex::new(Vec::with_capacity(6000)));
    let started = Instant::now();
    let mut writers = Vec::new();
    for client in 0..16u32 {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        let samples = Arc::clone(&samples);
        writers.push(std::thread::spawn(move || {
            barrier.wait();
            for batch in 0..25u32 {
                let mut committed_times = [Instant::now(); 15];
                for (offset, committed) in committed_times.iter_mut().enumerate() {
                    let sequence = batch * 15 + offset as u32;
                    let id = 4000 + client * 375 + sequence;
                    let text = if sequence % 100 == 0 {
                        format!("shared unique{id} {}", "x".repeat(9000))
                    } else {
                        format!("shared unique{id} {}", "x".repeat(1000))
                    };
                    engine
                        .exec(None, &format!("INSERT INTO t VALUES ({id}, '{text}')"))
                        .unwrap();
                    *committed = Instant::now();
                }
                // Queries run only after every writer has committed this batch,
                // so they exercise the inverted index instead of SeqScan fallback.
                barrier.wait();
                let last_id = 4000 + client * 375 + batch * 15 + 14;
                // The batch's last visible commit proves the whole preceding
                // prefix is covered, without an expensive OR-of-15 query.
                let result = engine
                    .exec(
                        None,
                        &format!("SELECT id FROM t WHERE content @@ 'unique{last_id}'"),
                    )
                    .unwrap();
                let QueryResult::Rows { rows, .. } = result else {
                    panic!("expected the batch's last visible commit")
                };
                assert_eq!(rows.len(), 1);
                let visible = Instant::now();
                for committed in committed_times {
                    samples
                        .lock()
                        .unwrap()
                        .push(visible.duration_since(committed));
                }
                barrier.wait();
                let target = Duration::from_millis(u64::from(batch + 1) * 2400);
                let elapsed = Instant::now().duration_since(started);
                if elapsed < target {
                    std::thread::sleep(target - elapsed);
                }
            }
        }));
    }
    let engine_for_queries = Arc::clone(&engine);
    let query_thread = std::thread::spawn(move || {
        let terms = ["shared", "unique4000", "unique9999", "missing"];
        for _ in 0..25 {
            for term in terms {
                let result = engine_for_queries
                    .exec(
                        None,
                        &format!("SELECT id FROM t WHERE content @@ '{term}' LIMIT 10"),
                    )
                    .unwrap();
                std::hint::black_box(result);
            }
        }
    });
    for writer in writers {
        writer.join().unwrap();
    }
    query_thread.join().unwrap();
    let elapsed = started.elapsed();
    let mut samples = samples.lock().unwrap().clone();
    assert_eq!(samples.len(), 6000);
    samples.sort();
    let p95 = samples[(samples.len() * 95) / 100];
    let rate = 6000.0 / elapsed.as_secs_f64();
    println!(
        "tier2 merge_enabled={merge_enabled}: docs=10000, clients=16, commits=6000, elapsed={elapsed:?}, rate={rate:.2}/s, commit_to_visible_p95={p95:?}"
    );
    assert!(p95 < Duration::from_secs(1), "p95 was {p95:?}");
    engine.shutdown();
}

#[test]
#[ignore = "release-only Tier 2 load benchmark with merge enabled"]
fn tier2_commit_visibility_p95_merge_enabled() {
    run_benchmark(true);
}

#[test]
#[ignore = "release-only Tier 2 load benchmark with merge disabled"]
fn tier2_commit_visibility_p95_merge_disabled() {
    run_benchmark(false);
}
