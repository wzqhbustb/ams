use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use pg_am_inverted::{parse_query, Analyzer, DefaultAnalyzer, InMemoryIndex};
use pg_storage::types::{PageId, Tid};
use serde_json::Value;
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, SchemaBuilder, TextFieldIndexing, TextOptions, Value as TantivyValue,
    STRING,
};
use tantivy::Term;
use tantivy::{doc, Index, TantivyDocument};

#[derive(Debug)]
struct Document {
    id: String,
    title: String,
    text: String,
}

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

fn documents() -> Vec<Document> {
    read_jsonl(&dataset_dir().join("corpus.jsonl"))
        .into_iter()
        .map(|row| Document {
            id: string_field(&row, "_id"),
            title: string_field(&row, "title"),
            text: string_field(&row, "text"),
        })
        .collect()
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

fn ndcg(ranked: &[String], relevant: &BTreeMap<String, f64>) -> f64 {
    let dcg = ranked
        .iter()
        .take(10)
        .enumerate()
        .map(|(index, id)| {
            let relevance = relevant.get(id).copied().unwrap_or(0.0);
            (2.0f64.powf(relevance) - 1.0) / ((index + 2) as f64).log2()
        })
        .sum::<f64>();
    let mut ideal = relevant.values().copied().collect::<Vec<_>>();
    ideal.sort_by(|a, b| b.total_cmp(a));
    let idcg = ideal
        .into_iter()
        .take(10)
        .enumerate()
        .map(|(index, relevance)| (2.0f64.powf(relevance) - 1.0) / ((index + 2) as f64).log2())
        .sum::<f64>();
    if idcg == 0.0 {
        0.0
    } else {
        dcg / idcg
    }
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

#[test]
#[ignore = "downloads are vendored in datasets/scifact; run explicitly for the quality gate"]
fn scifact_ndcg_matches_tantivy() {
    let documents = documents();
    assert!(!documents.is_empty());
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
    for document in &documents {
        writer
            .add_document(doc!(
                id_field => document.id.clone(),
                body_field => format!("{} {}", document.title, document.text)
            ))
            .unwrap();
    }
    writer.commit().unwrap();
    let reader = index.reader().unwrap();
    let searcher = reader.searcher();
    let mut inverted = InMemoryIndex::new(DefaultAnalyzer::mixed());
    let mut tid_to_id = BTreeMap::new();
    for (slot, document) in documents.iter().enumerate() {
        let tid = Tid {
            page_id: PageId(1),
            slot_id: slot as u16 + 1,
        };
        inverted
            .insert_document(tid, &format!("{} {}", document.title, document.text))
            .unwrap();
        tid_to_id.insert(tid, document.id.clone());
    }

    let mut differences = Vec::new();
    let mut evaluated = 0usize;
    for query in queries {
        let query_id = string_field(&query, "_id");
        let Some(relevant) = qrels.get(&query_id) else {
            continue;
        };
        let text = string_field(&query, "text");
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
        let top = searcher.search(&parsed, &TopDocs::with_limit(10)).unwrap();
        let tantivy_ids = top
            .into_iter()
            .map(|(_, address)| {
                let document: TantivyDocument = searcher.doc(address).unwrap();
                document
                    .get_first(id_field)
                    .unwrap()
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect::<Vec<_>>();
        let mine = inverted
            .search(&parse_query(inverted.analyzer(), &sanitized), Some(10))
            .unwrap()
            .into_iter()
            .map(|hit| tid_to_id[&hit.tid].clone())
            .collect::<Vec<_>>();
        let left = ndcg(&tantivy_ids, relevant);
        let right = ndcg(&mine, relevant);
        differences.push((query_id, left, right, (left - right).abs()));
        evaluated += 1;
    }
    assert!(evaluated > 0);
    let mean = differences.iter().map(|row| row.3).sum::<f64>() / evaluated as f64;
    differences.sort_by(|a, b| b.3.total_cmp(&a.3));
    println!(
        "SciFact NDCG@10: evaluated={evaluated}, mean_abs_diff={mean:.8}, worst={:?}",
        &differences[..differences.len().min(5)]
    );
    assert!(mean <= 0.005, "mean NDCG difference {mean} exceeds 0.005");
}
