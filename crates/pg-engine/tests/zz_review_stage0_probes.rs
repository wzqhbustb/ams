//! Regression tests for Stage 0 full-text parsing and fallback errors.
use pg_engine::sql::{self, CmpOp, IndexMethod, Literal, OrderByKind, Statement};
use pg_engine::{Engine, EngineConfig, EngineError};
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn extra_where_is_loudly_rejected_end_to_end() {
    let tmp = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(tmp.path(), EngineConfig::new(tmp.path())).unwrap());
    engine
        .exec(None, "CREATE TABLE t (id INT, content TEXT)")
        .unwrap();
    engine.create_index("t", "id").unwrap();
    engine
        .exec(None, "INSERT INTO t VALUES (1, 'hello'), (7, 'contract')")
        .unwrap();
    for sql in [
        "SELECT * FROM t WHERE id > 5 AND content @@ 'x'",
        "SELECT * FROM t WHERE content @@ 'x' AND id > 5",
        "EXPLAIN SELECT * FROM t WHERE id > 5 AND content @@ 'x'",
    ] {
        let error = engine.exec(None, sql).unwrap_err();
        assert!(
            matches!(error, EngineError::Unsupported(ref message) if message.contains("additional WHERE predicates")),
            "unexpected error for {sql}: {error:?}"
        );
    }
    engine.shutdown();
}

#[test]
fn uppercase_and_with_and_bm25_keywords_parse() {
    for sql in [
        "CREATE INDEX i ON t USING inverted (content) WITH (fulltext_index = 'bm25')",
        "CREATE INDEX i ON t USING inverted (content) with (fulltext_index = 'bm25')",
    ] {
        let stmt = sql::parse(sql).unwrap();
        let Statement::CreateIndex {
            method,
            fulltext_index,
            ..
        } = stmt
        else {
            panic!("expected inverted CREATE INDEX")
        };
        assert_eq!(method, Some(IndexMethod::Inverted));
        assert_eq!(fulltext_index.as_deref(), Some("bm25"));
    }

    for sql in [
        "SELECT id FROM t ORDER BY bm25_score(content, 'x') DESC LIMIT 5",
        "SELECT id FROM t ORDER BY BM25_SCORE(content, 'x') DESC LIMIT 5",
    ] {
        let stmt = sql::parse(sql).unwrap();
        let Statement::Select {
            order_by: Some(OrderByKind::Bm25Score(expr)),
            ..
        } = stmt
        else {
            panic!("expected BM25 ORDER BY")
        };
        assert_eq!(expr.column, "content");
        assert_eq!(expr.query, Literal::Str("x".to_string()));
        assert!(expr.desc);
    }

    let stmt = sql::parse("SELECT * FROM t WHERE content @@ 'x' AND id > 5").unwrap();
    let Statement::Select {
        filter: Some(fulltext),
        extra_filter: Some(extra),
        ..
    } = stmt
    else {
        panic!("expected ExtraWhere SELECT")
    };
    assert_eq!(fulltext.op, CmpOp::FullText);
    assert_eq!(fulltext.value, Literal::Str("x".to_string()));
    assert_eq!(extra.column, "id");
    assert_eq!(extra.op, CmpOp::Gt);
    assert_eq!(extra.value, Literal::Int(5));

    assert!(sql::parse("SELECT * FROM t WHERE content @@ 'x' AND id > 5 AND id < 100").is_err());
}
