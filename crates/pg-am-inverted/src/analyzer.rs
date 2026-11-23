//! Text analysis contracts shared by indexing and query execution.

use std::io::BufReader;
use std::sync::{Arc, OnceLock};

use jieba_rs::Jieba;
use rust_stemmers::{Algorithm, Stemmer};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{InvertedError, Result};

/// Stop-word list frozen into every built-in analyzer's configuration hash.
pub const BUILTIN_STOPWORDS: &[&str] = &[
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "in", "is", "it", "of", "on",
    "or", "the", "to", "with",
];

/// Vendored Chinese dictionary; no runtime download or host dictionary lookup.
pub const BUILTIN_DICTIONARY: &str = include_str!("../dict/custom.dict");

/// Analyzer identity persisted with an inverted index. Opening an index with
/// a different implementation, dictionary, or stop-word configuration is an
/// error rather than an invitation to mix posting formats.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnalyzerConfig {
    /// Stable analyzer name (`english`, `chinese`, or `mixed`).
    pub name: String,
    /// SHA256 of the complete analyzer/dictionary/stop-word configuration.
    pub config_hash: [u8; 32],
}

static ANALYZER_CONFIGS: OnceLock<[AnalyzerConfig; 3]> = OnceLock::new();

impl AnalyzerConfig {
    /// Build a named built-in configuration and bind it to the frozen assets.
    pub fn named(name: &str) -> Result<Self> {
        if !matches!(name, "english" | "chinese" | "mixed") {
            return Err(InvertedError::InvalidArgument(format!(
                "unknown analyzer name {name:?}"
            )));
        }
        let [english, chinese, mixed] = ANALYZER_CONFIGS.get_or_init(|| {
            [
                AnalyzerConfig {
                    name: "english".to_string(),
                    config_hash: compute_config_hash("english"),
                },
                AnalyzerConfig {
                    name: "chinese".to_string(),
                    config_hash: compute_config_hash("chinese"),
                },
                AnalyzerConfig {
                    name: "mixed".to_string(),
                    config_hash: compute_config_hash("mixed"),
                },
            ]
        });
        Ok(match name {
            "english" => english.clone(),
            "chinese" => chinese.clone(),
            "mixed" => mixed.clone(),
            _ => unreachable!("name was validated above"),
        })
    }

    /// English Snowball analyzer with the frozen stop-word list.
    pub fn english() -> Self {
        Self::named("english").expect("english is a registered analyzer")
    }

    /// Chinese jieba analyzer with the vendored dictionary.
    pub fn chinese() -> Self {
        Self::named("chinese").expect("chinese is a registered analyzer")
    }

    /// The Phase 3 built-in mixed analyzer configuration.
    pub fn builtin() -> Self {
        Self::named("mixed").expect("mixed is a registered analyzer")
    }

    /// Reject an unassigned or otherwise incomplete persisted identity.
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.name.as_str(), "english" | "chinese" | "mixed") {
            return Err(InvertedError::InvalidArgument(format!(
                "unknown analyzer name {:?}",
                self.name
            )));
        }
        if self.config_hash == [0; 32] {
            return Err(InvertedError::InvalidArgument(
                "analyzer configuration hash must not be zero".to_string(),
            ));
        }
        let expected = Self::named(&self.name)?;
        if self.config_hash != expected.config_hash {
            return Err(InvertedError::InvalidArgument(
                "analyzer configuration mismatch: name does not match configuration hash"
                    .to_string(),
            ));
        }
        Ok(())
    }
}

impl Default for AnalyzerConfig {
    fn default() -> Self {
        Self::builtin()
    }
}

/// Version of the Unicode segmentation and stemming rules. Changing these
/// rules requires a new persisted analyzer identity and an explicit rebuild.
pub const ANALYZER_FORMAT_VERSION: u16 = 1;

fn compute_config_hash(name: &str) -> [u8; 32] {
    compute_config_hash_with_dictionary(name, BUILTIN_DICTIONARY)
}

fn compute_config_hash_with_dictionary(name: &str, dictionary: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(ANALYZER_FORMAT_VERSION.to_le_bytes());
    hasher.update(name.as_bytes());
    hasher.update([0]);
    for word in BUILTIN_STOPWORDS {
        hasher.update(word.as_bytes());
        hasher.update([0]);
    }
    // English never reads the Chinese dictionary, so upgrading it cannot
    // invalidate an English index.
    if name != "english" {
        hasher.update(dictionary.as_bytes());
    }
    hasher.finalize().into()
}

/// One analyzed token and its zero-based position in the original text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    /// Token text after the analyzer's normalization rules.
    pub text: String,
    /// Zero-based token position used by future phrase queries. Stop-word
    /// removal does not renumber later tokens.
    pub position: u32,
}

/// Pluggable text analyzer. Indexing and query parsing must use the same
/// instance, dictionary, and configuration hash.
pub trait Analyzer: Send + Sync {
    /// Analyze `text` into ordered tokens. Empty tokens are filtered by the
    /// implementation, while positions remain contiguous before filtering.
    fn analyze(&self, text: &str) -> Vec<Token>;

    /// Exact analyzer identity used for persisted postings.
    fn config(&self) -> &AnalyzerConfig;
}

/// Built-in English, Chinese, and mixed analyzers backed by one frozen
/// dictionary and stop-word configuration.
pub struct DefaultAnalyzer {
    config: AnalyzerConfig,
    jieba: Arc<Jieba>,
    stemmer: Stemmer,
}

static SHARED_JIEBA: OnceLock<Arc<Jieba>> = OnceLock::new();
static SHARED_ANALYZERS: [OnceLock<Arc<DefaultAnalyzer>>; 3] =
    [OnceLock::new(), OnceLock::new(), OnceLock::new()];

impl DefaultAnalyzer {
    /// English Snowball analyzer with stop-word filtering.
    pub fn english() -> Self {
        Self::new(AnalyzerConfig::english())
    }

    /// Chinese jieba analyzer with the vendored dictionary.
    pub fn chinese() -> Self {
        Self::new(AnalyzerConfig::chinese())
    }

    /// Mixed CJK/Latin analyzer used by the default index configuration.
    pub fn mixed() -> Self {
        Self::new(AnalyzerConfig::builtin())
    }

    /// Construct or reuse the analyzer matching a persisted configuration.
    /// Only exact built-in identities share the process-wide instances.
    pub fn from_config(config: AnalyzerConfig) -> Arc<Self> {
        let Ok(expected) = AnalyzerConfig::named(&config.name) else {
            return Arc::new(Self::new(config));
        };
        if config != expected {
            return Arc::new(Self::new(config));
        }
        let slot = match config.name.as_str() {
            "english" => 0,
            "chinese" => 1,
            "mixed" => 2,
            _ => unreachable!("named accepted only built-in analyzer names"),
        };
        SHARED_ANALYZERS[slot]
            .get_or_init(|| Arc::new(Self::new(config)))
            .clone()
    }

    fn new(config: AnalyzerConfig) -> Self {
        let jieba = SHARED_JIEBA
            .get_or_init(|| {
                let mut jieba = Jieba::empty();
                jieba
                    .load_dict(&mut BufReader::new(BUILTIN_DICTIONARY.as_bytes()))
                    .expect("vendored jieba dictionary must be valid");
                Arc::new(jieba)
            })
            .clone();
        Self {
            config,
            jieba,
            stemmer: Stemmer::create(Algorithm::English),
        }
    }

    fn analyze_latin(&self, text: &str, stem: bool) -> Option<String> {
        let lower = text.to_lowercase();
        if BUILTIN_STOPWORDS.contains(&lower.as_str()) {
            return None;
        }
        if stem {
            Some(self.stemmer.stem(&lower).into_owned())
        } else {
            Some(lower)
        }
    }
}

impl Analyzer for DefaultAnalyzer {
    fn analyze(&self, text: &str) -> Vec<Token> {
        let mut tokens = Vec::new();
        let mut position = 0u32;
        for segment in segments(text) {
            match segment.kind {
                SegmentKind::Cjk => {
                    // English treats a contiguous CJK run as one opaque token.
                    if self.config.name == "english" {
                        tokens.push(Token {
                            text: segment.text.to_lowercase(),
                            position,
                        });
                        position += 1;
                        continue;
                    }
                    for word in self.jieba.cut(segment.text, false) {
                        // Empty jieba tokens do not consume positions; Latin
                        // stopwords intentionally leave position gaps.
                        if word.word.trim().is_empty() {
                            continue;
                        }
                        tokens.push(Token {
                            text: word.word.to_string(),
                            position,
                        });
                        position += 1;
                    }
                }
                SegmentKind::Latin => {
                    let stem = self.config.name != "chinese";
                    if let Some(text) = self.analyze_latin(segment.text, stem) {
                        tokens.push(Token { text, position });
                    }
                    position += 1;
                }
            }
        }
        tokens
    }

    fn config(&self) -> &AnalyzerConfig {
        &self.config
    }
}

impl Analyzer for Arc<DefaultAnalyzer> {
    fn analyze(&self, text: &str) -> Vec<Token> {
        self.as_ref().analyze(text)
    }

    fn config(&self) -> &AnalyzerConfig {
        self.as_ref().config()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SegmentKind {
    Cjk,
    Latin,
}

#[derive(Debug)]
struct Segment<'a> {
    kind: SegmentKind,
    text: &'a str,
}

fn segments(text: &str) -> Vec<Segment<'_>> {
    let mut result = Vec::new();
    let mut current: Option<(SegmentKind, usize, usize)> = None;
    for (index, ch) in text.char_indices() {
        let kind = classify(ch);
        match (current, kind) {
            (Some((current_kind, start, _)), Some(kind)) if current_kind == kind => {
                current = Some((current_kind, start, index + ch.len_utf8()));
            }
            (Some((current_kind, start, end)), next) => {
                result.push(Segment {
                    kind: current_kind,
                    text: &text[start..end],
                });
                current = next.map(|kind| (kind, index, index + ch.len_utf8()));
            }
            (None, next) => {
                current = next.map(|kind| (kind, index, index + ch.len_utf8()));
            }
        }
    }
    if let Some((kind, start, end)) = current {
        result.push(Segment {
            kind,
            text: &text[start..end],
        });
    }
    result
}

fn classify(ch: char) -> Option<SegmentKind> {
    if matches!(ch, '\u{3400}'..='\u{4dbf}' | '\u{4e00}'..='\u{9fff}' | '\u{f900}'..='\u{faff}') {
        Some(SegmentKind::Cjk)
    } else if ch.is_alphanumeric() || ch == '_' {
        Some(SegmentKind::Latin)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn english_analysis_stems_filters_and_preserves_positions() {
        let analyzer = DefaultAnalyzer::english();
        let tokens = analyzer.analyze("The contracts databases rust2026");
        assert_eq!(
            tokens,
            vec![
                Token {
                    text: "contract".to_string(),
                    position: 1,
                },
                Token {
                    text: "databas".to_string(),
                    position: 2,
                },
                Token {
                    text: "rust2026".to_string(),
                    position: 3,
                },
            ]
        );
    }

    #[test]
    fn chinese_and_mixed_analysis_use_vendored_dictionary() {
        let analyzer = DefaultAnalyzer::chinese();
        let tokens = analyzer.analyze("数据库 合同 rust2026");
        assert_eq!(
            tokens,
            vec![
                Token {
                    text: "数据库".to_string(),
                    position: 0,
                },
                Token {
                    text: "合同".to_string(),
                    position: 1,
                },
                Token {
                    text: "rust2026".to_string(),
                    position: 2,
                },
            ]
        );

        let mixed = DefaultAnalyzer::mixed();
        let tokens = mixed.analyze("全文检索 contracts 数据库");
        assert!(tokens.iter().any(|token| token.text == "全文检索"));
        assert!(tokens.iter().any(|token| token.text == "contract"));
        assert!(tokens.iter().any(|token| token.text == "数据库"));
    }

    #[test]
    fn configuration_hash_binds_name_stopwords_and_dictionary() {
        let mixed = AnalyzerConfig::builtin();
        assert_eq!(mixed.config_hash, compute_config_hash("mixed"));
        assert_ne!(mixed.config_hash, AnalyzerConfig::english().config_hash);
        assert_ne!(mixed.config_hash, AnalyzerConfig::chinese().config_hash);
        assert_ne!(mixed.config_hash, [0; 32]);
    }

    #[test]
    fn from_config_shares_only_exact_builtin_analyzers() {
        for config in [
            AnalyzerConfig::english(),
            AnalyzerConfig::chinese(),
            AnalyzerConfig::builtin(),
        ] {
            let first = DefaultAnalyzer::from_config(config.clone());
            let second = DefaultAnalyzer::from_config(config);
            assert!(Arc::ptr_eq(&first, &second));
        }

        let mut mismatched = AnalyzerConfig::builtin();
        mismatched.config_hash[0] ^= 1;
        let custom = DefaultAnalyzer::from_config(mismatched);
        let builtin = DefaultAnalyzer::from_config(AnalyzerConfig::builtin());
        assert!(!Arc::ptr_eq(&custom, &builtin));
    }

    #[test]
    fn all_constructors_share_the_jieba_dictionary() {
        let english = DefaultAnalyzer::english();
        let chinese = DefaultAnalyzer::chinese();
        let mixed = DefaultAnalyzer::mixed();
        let mut custom_config = AnalyzerConfig::builtin();
        custom_config.config_hash[0] ^= 1;
        let custom = DefaultAnalyzer::from_config(custom_config);
        assert!(Arc::ptr_eq(&english.jieba, &chinese.jieba));
        assert!(Arc::ptr_eq(&english.jieba, &mixed.jieba));
        assert!(Arc::ptr_eq(&english.jieba, &custom.jieba));
    }

    #[test]
    fn english_hash_excludes_unused_chinese_dictionary() {
        assert_eq!(
            compute_config_hash_with_dictionary("english", "different dictionary"),
            AnalyzerConfig::english().config_hash
        );
        assert_ne!(
            compute_config_hash_with_dictionary("mixed", "different dictionary"),
            AnalyzerConfig::builtin().config_hash
        );
        let mut mismatched = AnalyzerConfig::builtin();
        mismatched.name = "english".to_string();
        assert!(mismatched.validate().is_err());
    }

    #[test]
    fn out_of_vocabulary_chinese_uses_jieba_fallback() {
        let analyzer = DefaultAnalyzer::chinese();
        let tokens = analyzer.analyze("㐀㐁");
        assert_eq!(
            tokens,
            vec![
                Token {
                    text: "㐀".to_string(),
                    position: 0,
                },
                Token {
                    text: "㐁".to_string(),
                    position: 1,
                },
            ]
        );
    }
}
