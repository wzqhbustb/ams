//! Phase 4a Stage 0: the engine-side SQL session abstraction.
//!
//! The SQL session owns per-session, per-sequence state (`currval`) and, in
//! later stages, the SnapshotGuard owner. pg-wire's Session merely owns and
//! passes this context through — it does not own SQL semantics.

use std::collections::HashMap;

use pg_storage::types::Oid;

use crate::error::{EngineError, Result};

/// A per-session SQL context (Stage 0).
///
/// `currval` semantics follow PostgreSQL: for each sequence, the last value
/// returned by `nextval` **in this session** (not a global counter). A value
/// is never defined before that sequence's first `nextval` in this session.
pub struct SqlSession {
    /// Session identifier (unique per connection).
    pub id: u64,
    last_nextval: HashMap<Oid, i64>,
}

impl SqlSession {
    /// Create an empty SQL session with the given connection identifier.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            last_nextval: HashMap::new(),
        }
    }

    /// Record a `nextval` call's result for one sequence in this session.
    pub fn record_nextval(&mut self, sequence_oid: Oid, value: i64) {
        self.last_nextval.insert(sequence_oid, value);
    }

    /// `currval` semantics (PostgreSQL): the last value returned by
    /// `nextval` for this sequence in THIS session. Errors if that sequence
    /// was never advanced in this session.
    pub fn currval(&self, sequence_oid: Oid) -> Result<i64> {
        match self.last_nextval.get(&sequence_oid) {
            Some(v) => Ok(*v),
            None => Err(EngineError::InvalidArgument(format!(
                "currval is not defined until nextval is called in this session for sequence {sequence_oid}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn currval_errors_before_first_nextval() {
        let s = SqlSession::new(1);
        assert!(s.currval(Oid(10)).is_err());
    }

    #[test]
    fn currval_returns_last_nextval_in_this_session() {
        let mut s = SqlSession::new(1);
        let sequence = Oid(10);
        s.record_nextval(sequence, 41);
        assert_eq!(s.currval(sequence).unwrap(), 41);
        s.record_nextval(sequence, 42);
        assert_eq!(s.currval(sequence).unwrap(), 42);
    }

    #[test]
    fn currval_is_per_session_not_global() {
        let mut a = SqlSession::new(1);
        let b = SqlSession::new(2);
        let sequence = Oid(10);
        a.record_nextval(sequence, 7);
        assert_eq!(a.currval(sequence).unwrap(), 7);
        assert!(
            b.currval(sequence).is_err(),
            "another session must not see it"
        );
    }

    #[test]
    fn currval_is_independent_per_sequence() {
        let mut s = SqlSession::new(1);
        let seq_a = Oid(100);
        let seq_b = Oid(200);
        s.record_nextval(seq_a, 1);
        s.record_nextval(seq_b, 99);
        s.record_nextval(seq_a, 2);
        assert_eq!(s.currval(seq_a).unwrap(), 2);
        assert_eq!(s.currval(seq_b).unwrap(), 99);
    }
}
