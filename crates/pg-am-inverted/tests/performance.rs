use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use pg_am_inverted::{parse_query, Analyzer, DefaultAnalyzer, InMemoryIndex};
use pg_storage::types::{PageId, Tid};
use serde_json::Value;
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, SchemaBuilder, TextFieldIndexing, TextOptions, STRING,
};
use tantivy::{doc, Index, Term};

fn dataset_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../datasets/scifact")
}

fn read_jsonl(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn string_field(value: &Value, name: &str) -> String {
    value[name].as_str().unwrap().to_string()
}

fn boolean_query(field: Field, text: &str) -> Box<dyn Query> {
    let analyzer = DefaultAnalyzer::mixed();
    let mut groups = text
        .split(" OR ")
        .map(|group| {
            let terms = analyzer
                .analyze(group)
                .into_iter()
                .map(|token| {
                    (
                        Occur::Must,
                        Box::new(TermQuery::new(
                            Term::from_field_text(field, &token.text),
                            IndexRecordOption::WithFreqsAndPositions,
                        )) as Box<dyn Query>,
                    )
                })
                .collect::<Vec<_>>();
            (
                Occur::Should,
                Box::new(BooleanQuery::new(terms)) as Box<dyn Query>,
            )
        })
        .collect::<Vec<_>>();
    if groups.len() == 1 {
        groups.remove(0).1
    } else {
        Box::new(BooleanQuery::new(groups))
    }
}

fn qrels() -> HashMap<String, BTreeMap<String, f64>> {
    let mut result = HashMap::new();
    for line in std::fs::read_to_string(dataset_dir().join("test.tsv"))
        .unwrap()
        .lines()
        .skip(1)
    {
        let mut columns = line.split('\t');
        let query = columns.next().unwrap().to_string();
        let document = columns.next().unwrap().to_string();
        let score = columns.next().unwrap().parse::<f64>().unwrap();
        result
            .entry(query)
            .or_insert_with(BTreeMap::new)
            .insert(document, score);
    }
    result
}

fn percentile(mut values: Vec<Duration>, percentile: f64) -> Duration {
    values.sort();
    values[((values.len() - 1) as f64 * percentile) as usize]
}

#[test]
#[ignore = "release-only latency comparison against Tantivy"]
fn wand_latency_is_within_three_times_tantivy() {
    let documents = read_jsonl(&dataset_dir().join("corpus.jsonl"));
    let queries = read_jsonl(&dataset_dir().join("queries.jsonl"));
    let qrels = qrels();
    let tmp = tempfile::TempDir::new().unwrap();
    let body = TextOptions::default()
        .set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("en_stem")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions),
        )
        .set_stored();
    let mut schema_builder = SchemaBuilder::new();
    let body_field = schema_builder.add_text_field("body", body);
    let id_field = schema_builder.add_text_field("id", STRING.set_stored());
    let schema = schema_builder.build();
    let index = Index::create_in_dir(tmp.path(), schema).unwrap();
    let mut writer = index.writer(50_000_000).unwrap();
    for row in &documents {
        writer
            .add_document(doc!(
                id_field => string_field(row, "_id"),
                body_field => format!("{} {}", string_field(row, "title"), string_field(row, "text"))
            ))
            .unwrap();
    }
    writer.commit().unwrap();
    let searcher = index.reader().unwrap().searcher();
    let mut inverted = InMemoryIndex::new(DefaultAnalyzer::mixed());
    let mut tid_to_id = BTreeMap::new();
    for (slot, row) in documents.iter().enumerate() {
        let tid = Tid {
            page_id: PageId(1),
            slot_id: slot as u16 + 1,
        };
        inverted
            .insert_document(
                tid,
                &format!(
                    "{} {}",
                    string_field(row, "title"),
                    string_field(row, "text")
                ),
            )
            .unwrap();
        tid_to_id.insert(tid, string_field(row, "_id"));
    }
    let selected = queries
        .iter()
        .filter(|query| qrels.contains_key(&string_field(query, "_id")))
        .take(100)
        .collect::<Vec<_>>();
    assert_eq!(selected.len(), 100);
    let iterations = 5;
    let mut tantivy_times = Vec::new();
    let mut daat_times = Vec::new();
    let mut wand_times = Vec::new();
    for query in selected {
        let text = string_field(query, "text");
        let sanitized = text
            .chars()
            .map(|character| {
                if character.is_alphanumeric() || character.is_whitespace() {
                    character
                } else {
                    ' '
                }
            })
            .collect::<String>();
        let sanitized = sanitized
            .split_whitespace()
            .map(|word| {
                if word.eq_ignore_ascii_case("or") {
                    "OR"
                } else {
                    word
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        let parsed = boolean_query(body_field, &sanitized);
        let ours = parse_query(inverted.analyzer(), &sanitized);
        for _ in 0..iterations {
            let start = Instant::now();
            let rows = searcher.search(&parsed, &TopDocs::with_limit(10)).unwrap();
            tantivy_times.push(start.elapsed());
            std::hint::black_box(rows);
            let start = Instant::now();
            let rows = inverted.search(&ours, Some(10)).unwrap();
            daat_times.push(start.elapsed());
            std::hint::black_box(rows);
            let start = Instant::now();
            let rows = inverted.search_wand(&ours, Some(10)).unwrap();
            wand_times.push(start.elapsed());
            std::hint::black_box(rows);
        }
    }
    let tantivy_mean = tantivy_times.iter().sum::<Duration>() / tantivy_times.len() as u32;
    let daat_mean = daat_times.iter().sum::<Duration>() / daat_times.len() as u32;
    let wand_mean = wand_times.iter().sum::<Duration>() / wand_times.len() as u32;
    let tantivy_p95 = percentile(tantivy_times, 0.95);
    let daat_p95 = percentile(daat_times, 0.95);
    let wand_p95 = percentile(wand_times, 0.95);
    println!(
        "latency: tantivy mean={tantivy_mean:?} p95={tantivy_p95:?}; DAAT mean={daat_mean:?} p95={daat_p95:?}; WAND mean={wand_mean:?} p95={wand_p95:?}; ratios mean={:.3} p95={:.3}",
        wand_mean.as_secs_f64() / tantivy_mean.as_secs_f64(),
        wand_p95.as_secs_f64() / tantivy_p95.as_secs_f64()
    );
    assert!(
        wand_mean <= tantivy_mean * 3,
        "WAND mean latency {wand_mean:?} exceeds 3x Tantivy {tantivy_mean:?}"
    );
}
