//! Exact WAND evaluation with safe BM25 upper bounds.

use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use pg_storage::types::Tid;

use crate::analyzer::Analyzer;
use crate::bm25;
use crate::daat::{push_top_k, sort_results, InMemoryIndex, Query, ScoredTid, Worst};
use crate::error::Result;
use crate::posting::Posting;

/// Evaluate exact WAND for a bounded OR query. AND queries and unlimited
/// queries deliberately share the established DAAT path.
pub fn search<A: Analyzer>(
    index: &InMemoryIndex<A>,
    query: &Query,
    limit: Option<usize>,
) -> Result<Vec<ScoredTid>> {
    match limit {
        None => index.search(query, None),
        Some(0) => Ok(Vec::new()),
        Some(k) => match query {
            Query::And(_) => index.search(query, Some(k)),
            Query::Or(_) => search_or(index, query, k),
        },
    }
}

struct Cursor<'a> {
    iter: std::collections::btree_map::Iter<'a, Tid, Posting>,
    current: Option<Tid>,
    upper_bound: f64,
}

impl<'a> Cursor<'a> {
    fn new(
        postings: &'a BTreeMap<Tid, Posting>,
        doc_lengths: &BTreeMap<Tid, u64>,
        avg_doc_len: f64,
        doc_count: u64,
    ) -> Result<Self> {
        let doc_freq = postings.len() as u64;
        let upper_bound =
            postings
                .iter()
                .try_fold(f64::NEG_INFINITY, |bound, (tid, posting)| {
                    let Some(doc_len) = doc_lengths.get(tid).copied() else {
                        return Err(crate::InvertedError::Corrupted(format!(
                            "posting for {tid} has no document length"
                        )));
                    };
                    let score =
                        bm25::score(posting.term_freq, doc_len, avg_doc_len, doc_freq, doc_count)?;
                    Ok(bound.max(score))
                })?;
        let mut iter = postings.iter();
        let current = iter.next().map(|(tid, _)| *tid);
        Ok(Self {
            iter,
            current,
            upper_bound,
        })
    }

    fn advance_to(&mut self, target: Tid) {
        while self.current.is_some_and(|tid| tid < target) {
            self.current = self.iter.next().map(|(tid, _)| *tid);
        }
    }

    fn advance_past(&mut self, target: Tid) {
        while self.current.is_some_and(|tid| tid <= target) {
            self.current = self.iter.next().map(|(tid, _)| *tid);
        }
    }
}

fn search_or<A: Analyzer>(
    index: &InMemoryIndex<A>,
    query: &Query,
    k: usize,
) -> Result<Vec<ScoredTid>> {
    let avg_doc_len = if index.doc_lengths().is_empty() {
        0.0
    } else {
        index.stats().total_tokens as f64 / index.doc_lengths().len() as f64
    };
    let doc_count = index.doc_lengths().len() as u64;
    let Query::Or(groups) = query else {
        unreachable!("caller checked OR query")
    };
    let terms = groups.iter().flatten().collect::<BTreeSet<_>>();
    let mut cursors = Vec::new();
    for term in terms {
        if let Some(postings) = index.postings().get(term) {
            cursors.push(Cursor::new(
                postings,
                index.doc_lengths(),
                avg_doc_len,
                doc_count,
            )?);
        }
    }
    let mut heap = BinaryHeap::new();
    loop {
        cursors.retain(|cursor| cursor.current.is_some());
        if cursors.is_empty() {
            break;
        }
        cursors.sort_by_key(|cursor| cursor.current);
        let threshold = if heap.len() == k {
            heap.peek()
                .map_or(f64::NEG_INFINITY, |Worst(row)| row.score)
        } else {
            f64::NEG_INFINITY
        };
        let mut cumulative = 0.0;
        let mut pivot = None;
        for (index, cursor) in cursors.iter().enumerate() {
            cumulative += cursor.upper_bound;
            if cumulative >= threshold {
                pivot = Some(index);
                break;
            }
        }
        let Some(pivot) = pivot else {
            break;
        };
        let pivot_tid = cursors[pivot].current.expect("live cursor has a TID");
        if cursors[0].current == Some(pivot_tid) {
            for cursor in &mut cursors {
                if cursor.current == Some(pivot_tid) {
                    cursor.advance_past(pivot_tid);
                }
            }
            if !index.matches_query(pivot_tid, query) {
                continue;
            }
            let score = index.score_document(pivot_tid, query, avg_doc_len, doc_count)?;
            #[cfg(test)]
            record_score_evaluation();
            push_top_k(
                &mut heap,
                ScoredTid {
                    tid: pivot_tid,
                    score,
                },
                k,
            );
        } else {
            cursors[0].advance_to(pivot_tid);
        }
    }
    let mut rows = heap.into_iter().map(|Worst(row)| row).collect::<Vec<_>>();
    sort_results(&mut rows);
    Ok(rows)
}

#[cfg(test)]
thread_local! {
    static SCORE_EVALUATIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn record_score_evaluation() {
    SCORE_EVALUATIONS.with(|count| count.set(count.get() + 1));
}

#[cfg(test)]
fn reset_score_evaluations() {
    SCORE_EVALUATIONS.with(|count| count.set(0));
}

#[cfg(test)]
fn score_evaluations() -> usize {
    SCORE_EVALUATIONS.with(|count| count.get())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::DefaultAnalyzer;
    use crate::daat::parse_query;
    use pg_storage::types::PageId;

    fn tid(slot: u16) -> Tid {
        Tid {
            page_id: PageId(1),
            slot_id: slot,
        }
    }

    fn index_with_docs(docs: &[&str]) -> InMemoryIndex<DefaultAnalyzer> {
        let mut index = InMemoryIndex::new(DefaultAnalyzer::mixed());
        for (slot, text) in docs.iter().enumerate() {
            index.insert_document(tid(slot as u16 + 1), text).unwrap();
        }
        index
    }

    #[test]
    fn wand_matches_daat_for_and_or_empty_duplicate_and_tied_queries() {
        let index = index_with_docs(&[
            "contract rust",
            "contract database",
            "contract rust database",
            "unrelated",
            "contract",
        ]);
        for text in [
            "contract",
            "contract rust",
            "contract OR rust",
            "contract rust OR database",
            "contract OR contract",
            "missing",
            "",
        ] {
            let query = parse_query(index.analyzer(), text);
            for limit in [None, Some(0), Some(1), Some(2), Some(3), Some(usize::MAX)] {
                assert_eq!(
                    search(&index, &query, limit).unwrap(),
                    index.search(&query, limit).unwrap(),
                    "query={text:?}, limit={limit:?}"
                );
            }
        }
    }

    #[test]
    fn wand_prunes_using_the_kth_score_upper_bound() {
        let index = index_with_docs(&[
            "alpha bravo charlie delta",
            "alpha",
            "bravo",
            "charlie",
            "delta",
        ]);
        let query = parse_query(index.analyzer(), "alpha OR bravo OR charlie OR delta");
        reset_score_evaluations();
        let rows = search(&index, &query, Some(1)).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tid, tid(1));
        assert_eq!(score_evaluations(), 1);
        assert_eq!(rows, index.search(&query, Some(1)).unwrap());
    }

    #[test]
    fn wand_matches_daat_on_generated_boolean_corpus() {
        let terms = ["alpha", "bravo", "charlie", "delta"];
        let documents = (0..40)
            .map(|index| {
                let mut text = terms
                    .iter()
                    .enumerate()
                    .filter(|(term_index, _)| (index + term_index + 1) % 3 != 0)
                    .map(|(_, term)| *term)
                    .collect::<Vec<_>>()
                    .join(" ");
                if index % 5 == 0 {
                    text.push_str(" alpha alpha");
                }
                if index % 7 == 0 {
                    text.push_str(" unrelated");
                }
                text
            })
            .collect::<Vec<_>>();
        let refs = documents.iter().map(String::as_str).collect::<Vec<_>>();
        let index = index_with_docs(&refs);
        for text in [
            "alpha",
            "alpha OR bravo",
            "alpha bravo",
            "alpha bravo OR charlie",
            "alpha OR bravo OR charlie OR delta",
            "alpha OR alpha",
            "missing OR alpha",
            "missing OR also_missing",
        ] {
            let query = parse_query(index.analyzer(), text);
            for limit in [
                None,
                Some(0),
                Some(1),
                Some(2),
                Some(3),
                Some(5),
                Some(31),
                Some(usize::MAX),
            ] {
                assert_eq!(
                    search(&index, &query, limit).unwrap(),
                    index.search(&query, limit).unwrap(),
                    "query={text:?}, limit={limit:?}"
                );
            }
        }
    }

    #[test]
    fn wand_matches_daat_for_high_document_frequencies_and_ties() {
        let index = index_with_docs(&["contract", "contract", "contract", "contract", "database"]);
        let query = parse_query(index.analyzer(), "contract OR database");
        for limit in [Some(1), Some(2), Some(5), Some(usize::MAX)] {
            assert_eq!(
                search(&index, &query, limit).unwrap(),
                index.search(&query, limit).unwrap()
            );
        }
    }
}
