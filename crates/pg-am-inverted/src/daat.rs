//! In-memory posting lists, Boolean DAAT evaluation, and bounded top-k.

use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use pg_storage::types::{PageId, Tid};

use crate::analyzer::Analyzer;
use crate::bm25;
use crate::error::{InvertedError, Result};
use crate::posting::Posting;

/// One scored heap row returned by the in-memory DAAT evaluator.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoredTid {
    /// Heap tuple identifier.
    pub tid: Tid,
    /// Sum of the matching Boolean group's BM25 contributions.
    pub score: f64,
}

impl Eq for ScoredTid {}

impl PartialOrd for ScoredTid {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScoredTid {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.tid.cmp(&other.tid))
    }
}

/// Heap key that puts the worst retained result at the top: lowest score
/// first, then largest TID. `ScoredTid`'s own ordering is score ASC, tid ASC,
/// so the bounded heap never needs a second result-order comparator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Worst(ScoredTid);

impl PartialOrd for Worst {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Worst {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other
            .0
            .score
            .total_cmp(&self.0.score)
            .then_with(|| self.0.tid.cmp(&other.0.tid))
    }
}

/// A parsed Boolean query. Each inner vector is an AND group; `Or` unions
/// those groups. Repeated terms across the whole query are scored only once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Query {
    /// One conjunction, including a single-term query.
    And(Vec<String>),
    /// A disjunction of conjunctions.
    Or(Vec<Vec<String>>),
}

/// Parse the intentionally small Phase 3 query language: whitespace is AND,
/// case-insensitive `or` separates OR groups. Phrase, prefix, fuzzy, and
/// parenthesized queries are outside this contract.
pub fn parse_query<A: Analyzer>(analyzer: &A, text: &str) -> Query {
    let groups: Vec<Vec<String>> = split_boolean_or(text)
        .into_iter()
        .map(|part| analyzed_terms(analyzer, &part))
        .collect();
    if groups.len() == 1 {
        Query::And(groups.into_iter().next().unwrap_or_default())
    } else {
        Query::Or(groups)
    }
}

fn split_boolean_or(text: &str) -> Vec<String> {
    let mut groups = Vec::new();
    let mut current = String::new();
    for word in text.split_whitespace() {
        if word.eq_ignore_ascii_case("or") {
            groups.push(current);
            current = String::new();
        } else {
            if !current.is_empty() {
                current.push(' ');
            }
            current.push_str(word);
        }
    }
    groups.push(current);
    groups
}

fn analyzed_terms<A: Analyzer>(analyzer: &A, text: &str) -> Vec<String> {
    analyzer
        .analyze(text)
        .into_iter()
        .map(|token| token.text)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Statistics accumulated by the in-memory index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InMemoryStats {
    /// Number of documents, including zero-length documents.
    pub doc_count: u64,
    /// Total analyzed token count across all documents.
    pub total_tokens: u64,
    /// Number of inserted source rows.
    pub row_count: u64,
    /// Number of `(term, document)` posting records in the segment.
    pub posting_count: u64,
}

/// A single-writer, read-only-after-build in-memory inverted index used to
/// validate DAAT and BM25 before segment persistence is introduced.
pub struct InMemoryIndex<A: Analyzer> {
    analyzer: A,
    postings: BTreeMap<String, BTreeMap<Tid, Posting>>,
    doc_lengths: BTreeMap<Tid, u64>,
    total_tokens: u64,
    row_count: u64,
    posting_count: u64,
}

impl<A: Analyzer> InMemoryIndex<A> {
    /// Create an empty index using the supplied analyzer instance for both
    /// document and query analysis.
    pub fn new(analyzer: A) -> Self {
        Self {
            analyzer,
            postings: BTreeMap::new(),
            doc_lengths: BTreeMap::new(),
            total_tokens: 0,
            row_count: 0,
            posting_count: 0,
        }
    }

    /// Borrow the exact analyzer instance used for document analysis. Query
    /// parsing must use this reference rather than constructing another one.
    pub fn analyzer(&self) -> &A {
        &self.analyzer
    }

    /// Add one unique document. Duplicate TIDs are rejected instead of being
    /// silently double-counted in document statistics.
    pub fn insert_document(&mut self, tid: Tid, text: &str) -> Result<()> {
        if tid.page_id == PageId::INVALID {
            return Err(InvertedError::InvalidArgument(format!(
                "invalid document TID {tid:?}"
            )));
        }
        if self.doc_lengths.contains_key(&tid) {
            return Err(InvertedError::InvalidArgument(format!(
                "duplicate document TID {tid:?}"
            )));
        }
        let tokens = self.analyzer.analyze(text);
        ensure_token_count_within_u32(tokens.len())?;
        let mut previous_position = None;
        for token in &tokens {
            if token.text.trim().is_empty() {
                return Err(InvertedError::InvalidArgument(format!(
                    "analyzer returned an empty token at position {}",
                    token.position
                )));
            }
            if previous_position.is_some_and(|previous| token.position <= previous) {
                return Err(InvertedError::InvalidArgument(format!(
                    "analyzer returned non-increasing token position {}",
                    token.position
                )));
            }
            previous_position = Some(token.position);
        }
        let doc_len = tokens.len() as u64;
        let mut grouped: BTreeMap<String, Vec<u32>> = BTreeMap::new();
        for token in tokens {
            grouped.entry(token.text).or_default().push(token.position);
        }
        // Compute every frequency before mutating the index, so overflow or
        // truncation cannot leave a posting without its document length.
        let term_frequencies = grouped
            .iter()
            .map(|(term, positions)| {
                let existing = self
                    .postings
                    .get(term)
                    .and_then(|postings| postings.get(&tid))
                    .map_or(0, |posting| posting.term_freq);
                checked_term_freq(existing, positions.len())
                    .map(|frequency| (term.clone(), frequency))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        for (term, positions) in grouped {
            let posting = self
                .postings
                .entry(term.clone())
                .or_default()
                .entry(tid)
                .or_insert_with(|| Posting {
                    tid,
                    term_freq: 0,
                    positions: Vec::new(),
                });
            posting.term_freq = term_frequencies[&term];
            posting.positions.extend(positions);
            // One posting record per (term, document), not per token position.
            self.posting_count += 1;
        }
        self.doc_lengths.insert(tid, doc_len);
        self.total_tokens += doc_len;
        self.row_count += 1;
        Ok(())
    }

    /// Current segment-local statistics.
    pub fn stats(&self) -> InMemoryStats {
        InMemoryStats {
            doc_count: self.doc_lengths.len() as u64,
            total_tokens: self.total_tokens,
            row_count: self.row_count,
            posting_count: self.posting_count,
        }
    }

    /// Number of documents containing `term` in this in-memory segment.
    pub fn term_df(&self, term: &str) -> u64 {
        self.postings
            .get(term)
            .map_or(0, |postings| postings.len() as u64)
    }

    /// Return the analyzer configuration used by this index.
    pub fn analyzer_config(&self) -> &crate::AnalyzerConfig {
        self.analyzer.config()
    }

    /// Evaluate a parsed Boolean query with exact DAAT and BM25. `None`
    /// returns every match; `Some(k)` keeps a bounded top-k heap. Inconsistent
    /// index statistics are returned as `Corrupted` rather than hidden.
    pub fn search(&self, query: &Query, limit: Option<usize>) -> Result<Vec<ScoredTid>> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        let candidates = self.candidates(query);
        let avg_doc_len = if self.doc_lengths.is_empty() {
            0.0
        } else {
            self.total_tokens as f64 / self.doc_lengths.len() as f64
        };
        let doc_count = self.doc_lengths.len() as u64;
        match limit {
            Some(k) => {
                let mut heap = BinaryHeap::with_capacity(k.min(candidates.len()));
                for tid in candidates {
                    let score = self.score_document(tid, query, avg_doc_len, doc_count)?;
                    let scored = ScoredTid { tid, score };
                    if heap.len() < k {
                        heap.push(Worst(scored));
                    } else if let Some(Worst(worst)) = heap.peek() {
                        let replace = match scored.score.total_cmp(&worst.score) {
                            std::cmp::Ordering::Greater => true,
                            std::cmp::Ordering::Equal => scored.tid < worst.tid,
                            std::cmp::Ordering::Less => false,
                        };
                        if replace {
                            heap.pop();
                            heap.push(Worst(scored));
                        }
                    }
                }
                let mut rows = heap.into_iter().map(|Worst(row)| row).collect::<Vec<_>>();
                sort_results(&mut rows);
                Ok(rows)
            }
            None => {
                let mut rows = candidates
                    .into_iter()
                    .map(|tid| {
                        Ok(ScoredTid {
                            tid,
                            score: self.score_document(tid, query, avg_doc_len, doc_count)?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                sort_results(&mut rows);
                Ok(rows)
            }
        }
    }

    fn candidates(&self, query: &Query) -> BTreeSet<Tid> {
        match query {
            Query::And(terms) => self.and_candidates(terms),
            Query::Or(groups) => groups
                .iter()
                .filter(|terms| !terms.is_empty())
                .flat_map(|terms| self.and_candidates(terms))
                .collect(),
        }
    }

    fn and_candidates(&self, terms: &[String]) -> BTreeSet<Tid> {
        let Some((first, rest)) = terms.split_first() else {
            return BTreeSet::new();
        };
        let Some(postings) = self.postings.get(first) else {
            return BTreeSet::new();
        };
        let mut candidates = postings.keys().copied().collect::<BTreeSet<_>>();
        for term in rest {
            let Some(postings) = self.postings.get(term) else {
                return BTreeSet::new();
            };
            let term_tids = postings.keys().copied().collect::<BTreeSet<_>>();
            candidates = candidates
                .intersection(&term_tids)
                .copied()
                .collect::<BTreeSet<_>>();
            if candidates.is_empty() {
                break;
            }
        }
        candidates
    }

    fn score_document(
        &self,
        tid: Tid,
        query: &Query,
        avg_doc_len: f64,
        doc_count: u64,
    ) -> Result<f64> {
        let Some(doc_len) = self.doc_lengths.get(&tid).copied() else {
            return Err(InvertedError::Corrupted(format!(
                "posting for {tid:?} has no document length"
            )));
        };
        let mut total = 0.0;
        let unique_terms = query_groups(query)
            .into_iter()
            .flatten()
            .collect::<BTreeSet<_>>();
        for term in unique_terms {
            let Some(postings) = self.postings.get(term) else {
                continue;
            };
            let Some(posting) = postings.get(&tid) else {
                continue;
            };
            total += bm25::score(
                posting.term_freq,
                doc_len,
                avg_doc_len,
                postings.len() as u64,
                doc_count,
            )?;
        }
        Ok(total)
    }
}

fn ensure_token_count_within_u32(token_count: usize) -> Result<()> {
    if token_count > u32::MAX as usize {
        return Err(InvertedError::InvalidArgument(format!(
            "document has {token_count} tokens, exceeding the u32 position limit"
        )));
    }
    Ok(())
}

fn checked_term_freq(existing: u32, positions: usize) -> Result<u32> {
    let count = u32::try_from(positions).map_err(|_| {
        InvertedError::InvalidArgument("posting position count exceeds u32".to_string())
    })?;
    existing
        .checked_add(count)
        .ok_or_else(|| InvertedError::InvalidArgument("term frequency exceeds u32".to_string()))
}

fn query_groups(query: &Query) -> Vec<&[String]> {
    match query {
        Query::And(terms) => vec![terms.as_slice()],
        Query::Or(groups) => groups
            .iter()
            .filter(|terms| !terms.is_empty())
            .map(Vec::as_slice)
            .collect(),
    }
}

fn sort_results(rows: &mut [ScoredTid]) {
    rows.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.tid.cmp(&right.tid))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::DefaultAnalyzer;
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

    struct StubAnalyzer {
        config: crate::AnalyzerConfig,
        tokens: Vec<crate::analyzer::Token>,
    }

    impl Analyzer for StubAnalyzer {
        fn analyze(&self, _text: &str) -> Vec<crate::analyzer::Token> {
            self.tokens.clone()
        }

        fn config(&self) -> &crate::AnalyzerConfig {
            &self.config
        }
    }

    fn invalid_analyzer(tokens: Vec<crate::analyzer::Token>) -> StubAnalyzer {
        StubAnalyzer {
            config: crate::AnalyzerConfig::builtin(),
            tokens,
        }
    }

    #[test]
    fn and_or_and_single_term_queries_are_exact() {
        let index = index_with_docs(&[
            "contract rust",
            "contract database",
            "contract rust database",
            "unrelated",
        ]);
        let and = Query::And(vec!["contract".into(), "rust".into()]);
        assert_eq!(
            index
                .search(&and, None)
                .unwrap()
                .into_iter()
                .map(|row| row.tid)
                .collect::<Vec<_>>(),
            vec![tid(1), tid(3)]
        );
        let or = parse_query(index.analyzer(), "contract rust OR database");
        assert_eq!(
            index
                .search(&or, None)
                .unwrap()
                .into_iter()
                .map(|row| row.tid)
                .collect::<Vec<_>>(),
            vec![tid(3), tid(1), tid(2)]
        );
        assert!(index
            .search(&Query::And(vec!["missing".into()]), None)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn duplicate_query_terms_are_scored_once() {
        let index = index_with_docs(&["contract contract"]);
        let single = Query::And(vec!["contract".into()]);
        let duplicate = Query::And(vec!["contract".into(), "contract".into()]);
        assert_eq!(
            index.search(&single, None).unwrap(),
            index.search(&duplicate, None).unwrap()
        );
    }

    #[test]
    fn or_is_idempotent_for_duplicate_terms() {
        let index = index_with_docs(&["contract contract"]);
        let single = parse_query(index.analyzer(), "contract");
        let repeated = parse_query(index.analyzer(), "contract OR contract");
        assert_eq!(
            index.search(&single, None).unwrap(),
            index.search(&repeated, None).unwrap()
        );
    }

    #[test]
    fn lowercase_or_is_boolean_instead_of_becoming_and() {
        let index = index_with_docs(&["contract", "database"]);
        let query = parse_query(index.analyzer(), "contract or database");
        assert!(matches!(query, Query::Or(_)));
        let rows = index.search(&query, None).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn term_df_counts_documents_not_occurrences() {
        let index = index_with_docs(&["contract contract rust", "contract", "other"]);
        assert_eq!(index.term_df("contract"), 2);
        assert_eq!(index.term_df("rust"), 1);
        assert_eq!(index.term_df("other"), 1);
        assert_eq!(index.term_df("missing"), 0);
    }

    #[test]
    fn top_k_is_bounded_and_ties_break_by_tid() {
        let index = index_with_docs(&["contract", "contract", "contract"]);
        assert!(index
            .search(&Query::And(vec!["contract".into()]), Some(0))
            .unwrap()
            .is_empty());
        let first = index
            .search(&Query::And(vec!["contract".into()]), Some(1))
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].tid, tid(1));
        let all = index
            .search(&Query::And(vec!["contract".into()]), Some(usize::MAX))
            .unwrap();
        assert_eq!(all.len(), 3);
        assert!(all.windows(2).all(|pair| pair[0].tid < pair[1].tid));
        assert!(all.iter().all(|row| row.score == all[0].score));
    }

    #[test]
    fn zero_documents_and_zero_length_documents_are_safe() {
        let mut index = InMemoryIndex::new(DefaultAnalyzer::mixed());
        assert!(index
            .search(&Query::And(vec!["contract".into()]), None)
            .unwrap()
            .is_empty());
        index.insert_document(tid(1), "").unwrap();
        assert!(index
            .search(&Query::And(vec!["contract".into()]), None)
            .unwrap()
            .is_empty());
        assert_eq!(index.stats().doc_count, 1);
        assert_eq!(index.stats().total_tokens, 0);
        assert_eq!(index.stats().row_count, 1);
        assert_eq!(index.stats().posting_count, 0);
        assert!(index.insert_document(tid(1), "contract").is_err());
    }

    #[test]
    fn invalid_tid_is_rejected_without_mutation() {
        let mut index = InMemoryIndex::new(DefaultAnalyzer::mixed());
        let error = index
            .insert_document(
                Tid {
                    page_id: PageId::INVALID,
                    slot_id: 0,
                },
                "contract",
            )
            .unwrap_err();
        assert!(matches!(error, InvertedError::InvalidArgument(_)));
        assert_eq!(index.stats().doc_count, 0);
        assert_eq!(index.stats().row_count, 0);
        assert_eq!(index.stats().posting_count, 0);
    }

    #[test]
    fn invalid_analyzer_output_is_rejected_without_mutation() {
        for tokens in [
            vec![
                crate::analyzer::Token {
                    text: "contract".to_string(),
                    position: 0,
                },
                crate::analyzer::Token {
                    text: "rust".to_string(),
                    position: 0,
                },
            ],
            vec![crate::analyzer::Token {
                text: " ".to_string(),
                position: 0,
            }],
        ] {
            let mut index = InMemoryIndex::new(invalid_analyzer(tokens));
            let error = index.insert_document(tid(1), "contract").unwrap_err();
            assert!(matches!(error, InvertedError::InvalidArgument(_)));
            assert_eq!(index.stats().doc_count, 0);
            assert_eq!(index.stats().row_count, 0);
            assert_eq!(index.stats().posting_count, 0);
        }
    }

    #[test]
    fn token_count_and_frequency_overflows_are_rejected_atomically() {
        assert!(ensure_token_count_within_u32(u32::MAX as usize).is_ok());
        assert!(ensure_token_count_within_u32(u32::MAX as usize + 1).is_err());
        assert!(checked_term_freq(u32::MAX, 1).is_err());

        let mut index = InMemoryIndex::new(invalid_analyzer(vec![crate::analyzer::Token {
            text: "contract".to_string(),
            position: 0,
        }]));
        index.postings.insert(
            "contract".to_string(),
            BTreeMap::from([(
                tid(1),
                Posting {
                    tid: tid(1),
                    term_freq: u32::MAX,
                    positions: vec![0],
                },
            )]),
        );
        let error = index.insert_document(tid(1), "contract").unwrap_err();
        assert!(matches!(error, InvertedError::InvalidArgument(_)));
        assert_eq!(index.stats().doc_count, 0);
        assert_eq!(index.stats().row_count, 0);
        assert_eq!(index.stats().posting_count, 0);
        assert_eq!(index.postings["contract"][&tid(1)].term_freq, u32::MAX);
    }

    #[test]
    fn chinese_documents_are_searchable() {
        let index = index_with_docs(&["数据库 合同", "全文检索"]);
        let rows = index
            .search(&Query::And(vec!["数据库".into()]), None)
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tid, tid(1));
    }
}
