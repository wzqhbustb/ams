//! Minimal SQL parser: tokenizer + recursive descent (no external deps).
//!
//! Supports the M2b/M2c subset: BEGIN/COMMIT/ROLLBACK, CREATE TABLE,
//! INSERT, SELECT (with WHERE/ORDER BY/LIMIT and the M2c `FOR UPDATE` /
//! `FOR SHARE` lock clause), UPDATE, DELETE, CREATE INDEX. One statement
//! per `parse` call; a single trailing `;` is allowed and stripped.
//! Unquoted identifiers are lowercased (PostgreSQL fold-to-lower
//! semantics); quoted identifiers do not exist in this subset, so table
//! and column lookup is uniformly case-insensitive.
//!
//! # Not supported (by design, M2b/M2c)
//!
//! - `--` / `/* */` comments: `-` is only ever a negative-number sign,
//!   so `-- comment` fails with "invalid number: -" rather than being
//!   skipped.
//! - Quoted identifiers (`"MyTable"`), hence no case-sensitive or
//!   keyword-colliding names.
//! - Multi-statement input (only ONE optional trailing `;`).
//! - RETURNING, JOIN, subqueries, aggregates, GROUP BY/HAVING, `<=`/`>=`/
//!   `<>`/`!=` (only `=` / `<` / `>`), parenthesized expressions, and
//!   arithmetic in statements.
//! - `FOR NO KEY UPDATE` / `FOR KEY SHARE` (only the bare `FOR UPDATE` /
//!   `FOR SHARE` forms). `FOR SHARE` is fully implemented since M2c
//!   Stage S ("multixact lite"): it stamps a shared row lock
//!   (`HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_IS_SHARE`) on each result row.

#![allow(missing_docs)]

use pg_am_heap::tuple::ColumnType;

use crate::error::{EngineError, Result};

// ─── AST ──────────────────────────────────────────────────────────────

/// A parsed SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Begin,
    Commit,
    Rollback,
    CreateTable {
        name: String,
        columns: Vec<ColumnSpec>,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        rows: Vec<Vec<Literal>>,
    },
    Select {
        columns: SelectCols,
        table: String,
        filter: Option<Filter>,
        order_by: Option<OrderBy>,
        limit: Option<usize>,
        /// Row-lock clause (M2c Stage P): `FOR UPDATE` / `FOR SHARE`.
        lock: Option<LockClause>,
    },
    Update {
        table: String,
        sets: Vec<(String, Literal)>,
        filter: Option<Filter>,
    },
    Delete {
        table: String,
        filter: Option<Filter>,
    },
    CreateIndex {
        table: String,
        column: String,
    },
}

/// A SELECT row-lock clause (M2c Stage P, tech-selection §9.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockClause {
    /// `FOR UPDATE`: exclusive row lock via the lock-only `t_xmax` stamp.
    ForUpdate,
    /// `FOR SHARE`: shared row lock (M2c Stage S "multixact lite"):
    /// stamps `HEAP_XMAX_LOCK_ONLY | HEAP_XMAX_IS_SHARE` on each result
    /// row; the row stays visible to all snapshots.
    ForShare,
}

/// A column definition in CREATE TABLE.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSpec {
    pub name: String,
    pub col_type: ColumnType,
}

/// Which columns a SELECT returns.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectCols {
    All,
    Cols(Vec<String>),
}

/// A WHERE clause filter.
#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    pub column: String,
    pub op: CmpOp,
    pub value: Literal,
}

/// Comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Lt,
    Gt,
}

/// ORDER BY clause.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBy {
    pub column: String,
    pub desc: bool,
}

/// A literal value in SQL.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Int(i64),
    Str(String),
    Null,
}

// ─── Tokenizer ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Token {
    // Keywords
    Begin,
    Commit,
    Rollback,
    Create,
    Table,
    Insert,
    Into,
    Values,
    Select,
    From,
    Where,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    For,
    Share,
    Update,
    Set,
    Delete,
    Index,
    On,
    Null,
    // Type names
    Type(ColumnType),
    // Literals
    Int(i64),
    Str(String),
    // Operators
    Eq,
    Lt,
    Gt,
    // Punctuation
    Comma,
    Semicolon,
    LParen,
    RParen,
    Star,
    // Identifier (non-keyword)
    Ident(String),
    // End of input
    Eof,
}

struct Tokenizer<'a> {
    chars: std::str::Chars<'a>,
    peek: Option<char>,
}

impl<'a> Tokenizer<'a> {
    fn new(input: &'a str) -> Self {
        let mut chars = input.chars();
        let peek = chars.next();
        Self { chars, peek }
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek;
        self.peek = self.chars.next();
        c
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek {
            if c.is_whitespace() {
                self.bump();
            } else {
                break;
            }
        }
    }

    fn tokenize(mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            self.skip_whitespace();
            match self.peek {
                None => {
                    tokens.push(Token::Eof);
                    break;
                }
                Some(c) => {
                    if c == '\'' {
                        tokens.push(self.string_literal()?);
                    } else if c == '-' || c.is_ascii_digit() {
                        tokens.push(self.number_literal()?);
                    } else if c == '=' {
                        self.bump();
                        tokens.push(Token::Eq);
                    } else if c == '<' {
                        self.bump();
                        tokens.push(Token::Lt);
                    } else if c == '>' {
                        self.bump();
                        tokens.push(Token::Gt);
                    } else if c == ',' {
                        self.bump();
                        tokens.push(Token::Comma);
                    } else if c == ';' {
                        self.bump();
                        tokens.push(Token::Semicolon);
                    } else if c == '(' {
                        self.bump();
                        tokens.push(Token::LParen);
                    } else if c == ')' {
                        self.bump();
                        tokens.push(Token::RParen);
                    } else if c == '*' {
                        self.bump();
                        tokens.push(Token::Star);
                    } else if c.is_alphabetic() || c == '_' {
                        tokens.push(self.identifier_or_keyword());
                    } else {
                        return Err(EngineError::InvalidArgument(format!(
                            "unexpected character {c:?} in SQL"
                        )));
                    }
                }
            }
        }
        Ok(tokens)
    }

    fn string_literal(&mut self) -> Result<Token> {
        self.bump(); // opening quote
        let mut s = String::new();
        loop {
            match self.bump() {
                None => {
                    return Err(EngineError::InvalidArgument(
                        "unterminated string literal".to_string(),
                    ))
                }
                Some('\'') => {
                    // Doubled '' is an escaped quote
                    if self.peek == Some('\'') {
                        self.bump();
                        s.push('\'');
                    } else {
                        break;
                    }
                }
                Some(c) => s.push(c),
            }
        }
        Ok(Token::Str(s))
    }

    fn number_literal(&mut self) -> Result<Token> {
        let mut s = String::new();
        if self.peek == Some('-') {
            s.push(self.bump().unwrap());
        }
        while let Some(c) = self.peek {
            if c.is_ascii_digit() {
                s.push(self.bump().unwrap());
            } else {
                break;
            }
        }
        let n: i64 = s
            .parse()
            .map_err(|_| EngineError::InvalidArgument(format!("invalid number: {s}")))?;
        Ok(Token::Int(n))
    }

    fn identifier_or_keyword(&mut self) -> Token {
        let mut s = String::new();
        while let Some(c) = self.peek {
            if c.is_alphanumeric() || c == '_' {
                s.push(self.bump().unwrap());
            } else {
                break;
            }
        }
        match s.to_ascii_uppercase().as_str() {
            "BEGIN" => Token::Begin,
            "COMMIT" => Token::Commit,
            "ROLLBACK" => Token::Rollback,
            "CREATE" => Token::Create,
            "TABLE" => Token::Table,
            "INSERT" => Token::Insert,
            "INTO" => Token::Into,
            "VALUES" => Token::Values,
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "ORDER" => Token::Order,
            "BY" => Token::By,
            "ASC" => Token::Asc,
            "DESC" => Token::Desc,
            "LIMIT" => Token::Limit,
            "FOR" => Token::For,
            "SHARE" => Token::Share,
            "UPDATE" => Token::Update,
            "SET" => Token::Set,
            "DELETE" => Token::Delete,
            "INDEX" => Token::Index,
            "ON" => Token::On,
            "NULL" => Token::Null,
            "INT" | "INT4" | "INTEGER" => Token::Type(ColumnType::Int4),
            "BIGINT" | "INT8" => Token::Type(ColumnType::Int8),
            "TEXT" | "VARCHAR" => Token::Type(ColumnType::Text),
            "BYTEA" => Token::Type(ColumnType::Bytea),
            "TIMESTAMPTZ" => Token::Type(ColumnType::Timestamptz),
            "UUID" => Token::Type(ColumnType::Uuid),
            // Unquoted identifiers fold to lowercase (PostgreSQL semantics;
            // this subset has no quoted identifiers).
            _ => Token::Ident(s.to_ascii_lowercase()),
        }
    }
}

// ─── Parser ──────────────────────────────────────────────────────────

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn bump(&mut self) -> Token {
        let t = self.tokens[self.pos].clone();
        if self.pos + 1 < self.tokens.len() {
            self.pos += 1;
        }
        t
    }

    /// Consume the next token if it is `expected`.
    ///
    /// Comparison is by discriminant only, so `expected` MUST be a
    /// data-less (unit) variant: passing a data-carrying variant such as
    /// `Token::Ident("foo")` would match ANY `Token::Ident`, silently
    /// ignoring the payload. Every call site below honors this — keep it
    /// that way.
    fn eat(&mut self, expected: &Token) -> Result<()> {
        if std::mem::discriminant(self.peek()) == std::mem::discriminant(expected) {
            self.bump();
            Ok(())
        } else {
            Err(EngineError::InvalidArgument(format!(
                "expected {expected:?}, got {:?}",
                self.peek()
            )))
        }
    }

    fn eat_ident(&mut self) -> Result<String> {
        match self.bump() {
            Token::Ident(s) => Ok(s),
            other => Err(EngineError::InvalidArgument(format!(
                "expected identifier, got {other:?}"
            ))),
        }
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        match self.peek().clone() {
            Token::Begin => {
                self.bump();
                self.eat(&Token::Eof)?;
                Ok(Statement::Begin)
            }
            Token::Commit => {
                self.bump();
                self.eat(&Token::Eof)?;
                Ok(Statement::Commit)
            }
            Token::Rollback => {
                self.bump();
                self.eat(&Token::Eof)?;
                Ok(Statement::Rollback)
            }
            Token::Create => self.parse_create(),
            Token::Insert => self.parse_insert(),
            Token::Select => self.parse_select(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            other => Err(EngineError::InvalidArgument(format!(
                "unexpected token: {other:?}"
            ))),
        }
    }

    fn parse_create(&mut self) -> Result<Statement> {
        self.bump(); // CREATE
        match self.peek().clone() {
            Token::Table => self.parse_create_table(),
            Token::Index => self.parse_create_index(),
            other => Err(EngineError::InvalidArgument(format!(
                "expected TABLE or INDEX after CREATE, got {other:?}"
            ))),
        }
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        self.bump(); // TABLE
        let name = self.eat_ident()?;
        self.eat(&Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            let col_name = self.eat_ident()?;
            let col_type = match self.bump() {
                Token::Type(t) => t,
                other => {
                    return Err(EngineError::InvalidArgument(format!(
                        "expected column type, got {other:?}"
                    )))
                }
            };
            columns.push(ColumnSpec {
                name: col_name,
                col_type,
            });
            match self.peek() {
                Token::Comma => {
                    self.bump();
                }
                Token::RParen => {
                    self.bump();
                    break;
                }
                other => {
                    return Err(EngineError::InvalidArgument(format!(
                        "expected ',' or ')', got {other:?}"
                    )))
                }
            }
        }
        self.eat(&Token::Eof)?;
        Ok(Statement::CreateTable { name, columns })
    }

    fn parse_create_index(&mut self) -> Result<Statement> {
        self.bump(); // INDEX
        self.eat(&Token::On)?;
        let table = self.eat_ident()?;
        self.eat(&Token::LParen)?;
        let column = self.eat_ident()?;
        self.eat(&Token::RParen)?;
        self.eat(&Token::Eof)?;
        Ok(Statement::CreateIndex { table, column })
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.bump(); // INSERT
        self.eat(&Token::Into)?;
        let table = self.eat_ident()?;
        let columns = if matches!(self.peek(), Token::LParen) {
            self.bump();
            let mut cols = Vec::new();
            loop {
                cols.push(self.eat_ident()?);
                match self.peek() {
                    Token::Comma => {
                        self.bump();
                    }
                    Token::RParen => {
                        self.bump();
                        break;
                    }
                    other => {
                        return Err(EngineError::InvalidArgument(format!(
                            "expected ',' or ')', got {other:?}"
                        )))
                    }
                }
            }
            Some(cols)
        } else {
            None
        };
        self.eat(&Token::Values)?;
        let mut rows = Vec::new();
        loop {
            self.eat(&Token::LParen)?;
            let mut row = Vec::new();
            loop {
                row.push(self.parse_literal()?);
                match self.peek() {
                    Token::Comma => {
                        self.bump();
                    }
                    Token::RParen => {
                        self.bump();
                        break;
                    }
                    other => {
                        return Err(EngineError::InvalidArgument(format!(
                            "expected ',' or ')', got {other:?}"
                        )))
                    }
                }
            }
            rows.push(row);
            match self.peek() {
                Token::Comma => {
                    self.bump();
                }
                Token::Eof => break,
                other => {
                    return Err(EngineError::InvalidArgument(format!(
                        "expected ',' or end, got {other:?}"
                    )))
                }
            }
        }
        self.eat(&Token::Eof)?;
        Ok(Statement::Insert {
            table,
            columns,
            rows,
        })
    }

    fn parse_literal(&mut self) -> Result<Literal> {
        match self.bump() {
            Token::Int(n) => Ok(Literal::Int(n)),
            Token::Str(s) => Ok(Literal::Str(s)),
            Token::Null => Ok(Literal::Null),
            other => Err(EngineError::InvalidArgument(format!(
                "expected literal, got {other:?}"
            ))),
        }
    }

    fn parse_select(&mut self) -> Result<Statement> {
        self.bump(); // SELECT
        let columns = self.parse_select_cols()?;
        self.eat(&Token::From)?;
        let table = self.eat_ident()?;
        let filter = self.parse_optional_filter()?;
        let order_by = self.parse_optional_order_by()?;
        let limit = self.parse_optional_limit()?;
        let lock = self.parse_optional_lock_clause()?;
        self.eat(&Token::Eof)?;
        Ok(Statement::Select {
            columns,
            table,
            filter,
            order_by,
            limit,
            lock,
        })
    }

    /// The M2c row-lock clause (§9.1): `FOR UPDATE` / `FOR SHARE`, after
    /// LIMIT and before the statement end. `FOR NO KEY UPDATE` and
    /// `FOR KEY SHARE` are out of the subset.
    fn parse_optional_lock_clause(&mut self) -> Result<Option<LockClause>> {
        if !matches!(self.peek(), Token::For) {
            return Ok(None);
        }
        self.bump(); // FOR
        match self.bump() {
            Token::Update => Ok(Some(LockClause::ForUpdate)),
            Token::Share => Ok(Some(LockClause::ForShare)),
            other => Err(EngineError::InvalidArgument(format!(
                "expected UPDATE or SHARE after FOR, got {other:?}"
            ))),
        }
    }

    fn parse_select_cols(&mut self) -> Result<SelectCols> {
        match self.peek() {
            Token::Star => {
                self.bump();
                Ok(SelectCols::All)
            }
            Token::Ident(_) => {
                let mut cols = vec![self.eat_ident()?];
                while matches!(self.peek(), Token::Comma) {
                    self.bump();
                    cols.push(self.eat_ident()?);
                }
                Ok(SelectCols::Cols(cols))
            }
            other => Err(EngineError::InvalidArgument(format!(
                "expected column list or '*', got {other:?}"
            ))),
        }
    }

    fn parse_optional_filter(&mut self) -> Result<Option<Filter>> {
        if !matches!(self.peek(), Token::Where) {
            return Ok(None);
        }
        self.bump(); // WHERE
        let column = self.eat_ident()?;
        let op = match self.bump() {
            Token::Eq => CmpOp::Eq,
            Token::Lt => CmpOp::Lt,
            Token::Gt => CmpOp::Gt,
            other => {
                return Err(EngineError::InvalidArgument(format!(
                    "expected comparison operator, got {other:?}"
                )))
            }
        };
        let value = self.parse_literal()?;
        Ok(Some(Filter { column, op, value }))
    }

    fn parse_optional_order_by(&mut self) -> Result<Option<OrderBy>> {
        if !matches!(self.peek(), Token::Order) {
            return Ok(None);
        }
        self.bump(); // ORDER
        self.eat(&Token::By)?;
        let column = self.eat_ident()?;
        let desc = match self.peek() {
            Token::Asc => {
                self.bump();
                false
            }
            Token::Desc => {
                self.bump();
                true
            }
            _ => false,
        };
        Ok(Some(OrderBy { column, desc }))
    }

    fn parse_optional_limit(&mut self) -> Result<Option<usize>> {
        if !matches!(self.peek(), Token::Limit) {
            return Ok(None);
        }
        self.bump(); // LIMIT
        match self.bump() {
            Token::Int(n) if n >= 0 => Ok(Some(n as usize)),
            other => Err(EngineError::InvalidArgument(format!(
                "expected non-negative integer after LIMIT, got {other:?}"
            ))),
        }
    }

    fn parse_update(&mut self) -> Result<Statement> {
        self.bump(); // UPDATE
        let table = self.eat_ident()?;
        self.eat(&Token::Set)?;
        let mut sets = Vec::new();
        loop {
            let col = self.eat_ident()?;
            self.eat(&Token::Eq)?;
            let val = self.parse_literal()?;
            sets.push((col, val));
            match self.peek() {
                Token::Comma => {
                    self.bump();
                }
                _ => break,
            }
        }
        let filter = self.parse_optional_filter()?;
        self.eat(&Token::Eof)?;
        Ok(Statement::Update {
            table,
            sets,
            filter,
        })
    }

    fn parse_delete(&mut self) -> Result<Statement> {
        self.bump(); // DELETE
        self.eat(&Token::From)?;
        let table = self.eat_ident()?;
        let filter = self.parse_optional_filter()?;
        self.eat(&Token::Eof)?;
        Ok(Statement::Delete { table, filter })
    }
}

/// Parse a single SQL statement. One optional trailing `;` is allowed.
pub fn parse(sql: &str) -> Result<Statement> {
    let mut tokens = Tokenizer::new(sql).tokenize()?;
    // Strip one optional trailing semicolon before end of input.
    if tokens.len() >= 2 && tokens[tokens.len() - 2] == Token::Semicolon {
        tokens.remove(tokens.len() - 2);
    }
    let mut parser = Parser::new(tokens);
    parser.parse_statement()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_begin() {
        assert_eq!(parse("BEGIN").unwrap(), Statement::Begin);
    }

    #[test]
    fn parse_create_table() {
        let stmt = parse("CREATE TABLE users (id INT, name TEXT)").unwrap();
        match stmt {
            Statement::CreateTable { name, columns } => {
                assert_eq!(name, "users");
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[0].col_type, ColumnType::Int4);
                assert_eq!(columns[1].name, "name");
                assert_eq!(columns[1].col_type, ColumnType::Text);
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
    }

    #[test]
    fn parse_insert() {
        let stmt = parse("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        match stmt {
            Statement::Insert {
                table,
                columns,
                rows,
            } => {
                assert_eq!(table, "users");
                assert!(columns.is_none());
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].len(), 2);
                assert_eq!(rows[0][0], Literal::Int(1));
                assert_eq!(rows[0][1], Literal::Str("Alice".to_string()));
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }

    #[test]
    fn parse_insert_multiple_rows() {
        let stmt = parse("INSERT INTO t VALUES (1, 'a'), (2, 'b')").unwrap();
        match stmt {
            Statement::Insert { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn parse_select_all() {
        let stmt = parse("SELECT * FROM users").unwrap();
        match stmt {
            Statement::Select {
                columns,
                table,
                filter,
                order_by,
                limit,
                lock,
            } => {
                assert_eq!(columns, SelectCols::All);
                assert_eq!(table, "users");
                assert!(filter.is_none());
                assert!(order_by.is_none());
                assert!(limit.is_none());
                assert!(lock.is_none());
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parse_select_where_order_limit() {
        let stmt =
            parse("SELECT id, name FROM users WHERE id > 10 ORDER BY name DESC LIMIT 5").unwrap();
        match stmt {
            Statement::Select {
                columns,
                table,
                filter,
                order_by,
                limit,
                lock,
            } => {
                assert_eq!(
                    columns,
                    SelectCols::Cols(vec!["id".to_string(), "name".to_string()])
                );
                assert_eq!(table, "users");
                let f = filter.unwrap();
                assert_eq!(f.column, "id");
                assert_eq!(f.op, CmpOp::Gt);
                assert_eq!(f.value, Literal::Int(10));
                let ob = order_by.unwrap();
                assert_eq!(ob.column, "name");
                assert!(ob.desc);
                assert_eq!(limit, Some(5));
                assert!(lock.is_none());
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parse_select_for_update() {
        let stmt = parse("SELECT * FROM t WHERE id = 1 FOR UPDATE").unwrap();
        match stmt {
            Statement::Select { lock, filter, .. } => {
                assert_eq!(lock, Some(LockClause::ForUpdate));
                assert!(filter.is_some());
            }
            other => panic!("expected Select, got {other:?}"),
        }
        // The lock clause sits after LIMIT; a trailing semicolon still works.
        let stmt = parse("SELECT id FROM t LIMIT 3 FOR UPDATE;").unwrap();
        match stmt {
            Statement::Select { lock, limit, .. } => {
                assert_eq!(lock, Some(LockClause::ForUpdate));
                assert_eq!(limit, Some(3));
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parse_select_for_share() {
        let stmt = parse("SELECT * FROM t FOR SHARE").unwrap();
        match stmt {
            Statement::Select { lock, .. } => assert_eq!(lock, Some(LockClause::ForShare)),
            other => panic!("expected Select, got {other:?}"),
        }
        // FOR alone / FOR with an unknown mode is a parse error.
        assert!(parse("SELECT * FROM t FOR").is_err());
        assert!(parse("SELECT * FROM t FOR NO KEY UPDATE").is_err());
    }

    #[test]
    fn parse_update() {
        let stmt = parse("UPDATE users SET name = 'Bob' WHERE id = 1").unwrap();
        match stmt {
            Statement::Update {
                table,
                sets,
                filter,
            } => {
                assert_eq!(table, "users");
                assert_eq!(sets.len(), 1);
                assert_eq!(sets[0].0, "name");
                assert_eq!(sets[0].1, Literal::Str("Bob".to_string()));
                let f = filter.unwrap();
                assert_eq!(f.column, "id");
                assert_eq!(f.op, CmpOp::Eq);
                assert_eq!(f.value, Literal::Int(1));
            }
            other => panic!("expected Update, got {other:?}"),
        }
    }

    #[test]
    fn parse_delete() {
        let stmt = parse("DELETE FROM users WHERE id < 5").unwrap();
        match stmt {
            Statement::Delete { table, filter } => {
                assert_eq!(table, "users");
                let f = filter.unwrap();
                assert_eq!(f.column, "id");
                assert_eq!(f.op, CmpOp::Lt);
            }
            other => panic!("expected Delete, got {other:?}"),
        }
    }

    #[test]
    fn parse_create_index() {
        let stmt = parse("CREATE INDEX ON users (email)").unwrap();
        match stmt {
            Statement::CreateIndex { table, column } => {
                assert_eq!(table, "users");
                assert_eq!(column, "email");
            }
            other => panic!("expected CreateIndex, got {other:?}"),
        }
    }

    #[test]
    fn parse_negative_int() {
        let stmt = parse("SELECT * FROM t WHERE v < -5").unwrap();
        match stmt {
            Statement::Select {
                filter: Some(f), ..
            } => {
                assert_eq!(f.value, Literal::Int(-5));
            }
            _ => panic!("expected Select with filter"),
        }
    }

    #[test]
    fn parse_trailing_semicolon_allowed() {
        let stmt = parse("SELECT * FROM t;").unwrap();
        match stmt {
            Statement::Select { table, .. } => assert_eq!(table, "t"),
            other => panic!("expected Select, got {other:?}"),
        }
        // A second semicolon is still an error (one statement per parse).
        assert!(parse("SELECT * FROM t;;").is_err());
        // A semicolon in the middle is an error too.
        assert!(parse("SELECT * FROM t; SELECT * FROM t").is_err());
    }

    #[test]
    fn parse_identifiers_fold_to_lowercase() {
        let stmt = parse("CREATE TABLE Users (ID INT, Name TEXT)").unwrap();
        match stmt {
            Statement::CreateTable { name, columns } => {
                assert_eq!(name, "users");
                assert_eq!(columns[0].name, "id");
                assert_eq!(columns[1].name, "name");
            }
            other => panic!("expected CreateTable, got {other:?}"),
        }
        let stmt = parse("SELECT Id FROM Users WHERE ID = 1 ORDER BY Name").unwrap();
        match stmt {
            Statement::Select {
                columns,
                table,
                filter,
                order_by,
                ..
            } => {
                assert_eq!(columns, SelectCols::Cols(vec!["id".to_string()]));
                assert_eq!(table, "users");
                assert_eq!(filter.unwrap().column, "id");
                assert_eq!(order_by.unwrap().column, "name");
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }
}
