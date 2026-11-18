//! BM25 scoring primitives shared by index and exact fallback paths.

use crate::error::{InvertedError, Result};

/// BM25 term-saturation parameter, matching Tantivy/Lucene.
pub const K1: f64 = 1.2;
/// BM25 document-length normalization parameter, matching Tantivy/Lucene.
pub const B: f64 = 0.75;

/// Compute BM25 inverse document frequency.
///
/// `N = (doc_count - doc_freq + 0.5) / (doc_freq + 0.5)` with `ln(1 + N)`.
/// A zero document frequency contributes zero. `doc_count < doc_freq` is
/// inconsistent metadata and is rejected instead of being hidden as zero.
pub fn idf(doc_freq: u64, doc_count: u64) -> Result<f64> {
    if doc_count < doc_freq {
        return Err(InvertedError::Corrupted(format!(
            "document frequency {doc_freq} exceeds document count {doc_count}"
        )));
    }
    if doc_freq == 0 {
        return Ok(0.0);
    }
    let x = ((doc_count - doc_freq) as f64 + 0.5) / (doc_freq as f64 + 0.5);
    Ok(x.ln_1p())
}

/// Compute one term's BM25 contribution for one document.
///
/// Zero term frequency, zero document frequency, and zero average length are
/// valid and return zero. Impossible statistics return `Corrupted` instead of
/// silently becoming a valid zero score.
pub fn score(
    term_freq: u32,
    doc_len: u64,
    avg_doc_len: f64,
    doc_freq: u64,
    doc_count: u64,
) -> Result<f64> {
    if doc_count < doc_freq {
        return Err(InvertedError::Corrupted(format!(
            "document frequency {doc_freq} exceeds document count {doc_count}"
        )));
    }
    if !avg_doc_len.is_finite() || avg_doc_len < 0.0 {
        return Err(InvertedError::Corrupted(format!(
            "invalid average document length {avg_doc_len}"
        )));
    }
    if doc_count == 0 && term_freq > 0 {
        return Err(InvertedError::Corrupted(
            "positive term frequency with zero documents".to_string(),
        ));
    }
    if doc_len == 0 && term_freq > 0 {
        return Err(InvertedError::Corrupted(
            "positive term frequency in a zero-length document".to_string(),
        ));
    }
    if term_freq == 0 || doc_freq == 0 || avg_doc_len == 0.0 {
        return Ok(0.0);
    }
    let tf = term_freq as f64;
    let norm = K1 * (1.0 - B + B * doc_len as f64 / avg_doc_len);
    Ok(idf(doc_freq, doc_count)? * tf * (K1 + 1.0) / (tf + norm))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::{fieldnorm::FieldNormReader, query::Bm25Weight};

    #[test]
    fn scores_match_tantivy_bm25_weight() {
        for (term_freq, doc_len, avg_doc_len, doc_freq, doc_count) in [
            (1, 1, 20.0, 1, 2),
            (2, 10, 20.0, 1, 2),
            (3, 100, 20.0, 2, 4),
            (1, 5, 7.5, 3, 10),
        ] {
            let fieldnorm_id = FieldNormReader::fieldnorm_to_id(doc_len as u32);
            let quantized_doc_len = FieldNormReader::id_to_fieldnorm(fieldnorm_id) as u64;
            let actual = score(
                term_freq,
                quantized_doc_len,
                avg_doc_len,
                doc_freq,
                doc_count,
            )
            .unwrap();
            let expected =
                Bm25Weight::for_one_term(doc_freq, doc_count, avg_doc_len as tantivy::Score)
                    .score(fieldnorm_id, term_freq) as f64;
            assert!(
                (actual - expected).abs() < 1e-6,
                "tf={term_freq} dl={doc_len} avg={avg_doc_len} df={doc_freq} N={doc_count}: actual={actual} expected={expected}"
            );
        }
    }

    #[test]
    fn valid_zero_inputs_return_zero() {
        assert_eq!(score(0, 10, 20.0, 1, 2).unwrap(), 0.0);
        assert_eq!(score(1, 10, 20.0, 0, 2).unwrap(), 0.0);
        assert_eq!(score(1, 10, 0.0, 1, 2).unwrap(), 0.0);
        assert_eq!(score(0, 0, 0.0, 0, 0).unwrap(), 0.0);
        assert_eq!(idf(0, 0).unwrap(), 0.0);
        assert_eq!(idf(0, 2).unwrap(), 0.0);
    }

    #[test]
    fn impossible_statistics_are_corrupted() {
        assert!(matches!(
            score(1, 0, 20.0, 1, 2),
            Err(InvertedError::Corrupted(_))
        ));
        assert!(matches!(
            score(1, 10, f64::NAN, 1, 2),
            Err(InvertedError::Corrupted(_))
        ));
        assert!(matches!(
            score(1, 10, f64::INFINITY, 1, 2),
            Err(InvertedError::Corrupted(_))
        ));
        assert!(matches!(
            score(1, 10, -1.0, 1, 2),
            Err(InvertedError::Corrupted(_))
        ));
        assert!(matches!(
            score(1, 10, 20.0, 0, 0),
            Err(InvertedError::Corrupted(_))
        ));
        assert!(matches!(idf(3, 2), Err(InvertedError::Corrupted(_))));
    }
}
