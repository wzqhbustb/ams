//! BM25 scoring primitives shared by index and exact fallback paths.

/// BM25 term-saturation parameter, matching Tantivy/Lucene.
pub const K1: f64 = 1.2;
/// BM25 document-length normalization parameter, matching Tantivy/Lucene.
pub const B: f64 = 0.75;

/// Compute BM25 inverse document frequency.
///
/// `N = (doc_count - doc_freq + 0.5) / (doc_freq + 0.5)` with `ln(1 + N)`.
/// A zero document frequency contributes zero instead of panicking.
pub fn idf(doc_freq: u64, doc_count: u64) -> f64 {
    if doc_freq == 0 || doc_count < doc_freq {
        return 0.0;
    }
    let x = ((doc_count - doc_freq) as f64 + 0.5) / (doc_freq as f64 + 0.5);
    x.ln_1p()
}

/// Compute one term's BM25 contribution for one document.
///
/// Returns zero for empty terms, empty documents, zero average length, or
/// invalid statistics instead of producing a NaN.
pub fn score(term_freq: u32, doc_len: u64, avg_doc_len: f64, doc_freq: u64, doc_count: u64) -> f64 {
    if term_freq == 0 || doc_len == 0 || !avg_doc_len.is_finite() || avg_doc_len <= 0.0 {
        return 0.0;
    }
    let tf = term_freq as f64;
    let norm = K1 * (1.0 - B + B * doc_len as f64 / avg_doc_len);
    idf(doc_freq, doc_count) * tf * (K1 + 1.0) / (tf + norm)
}
