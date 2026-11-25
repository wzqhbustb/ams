//! Minimal SQL parser: tokenizer + recursive descent (no external deps).
//!
//! Supports the M2b/M2c subset plus Phase 4a Stage 0 syntax: BEGIN/COMMIT/
//! ROLLBACK, CREATE TABLE, INSERT, SELECT (with WHERE/ORDER BY/LIMIT and the
//! M2c `FOR UPDATE` / `FOR SHARE` lock clause), UPDATE, DELETE, and CREATE
//! INDEX with optional name/USING/opclass. ORDER BY also accepts the three
//! vector distance operators. One statement per `parse` call; a single
//! trailing `;` is allowed and stripped. Unquoted identifiers are lowercased
//! (PostgreSQL fold-to-lower semantics); quoted identifiers do not exist in
//! this subset, so table and column lookup is uniformly case-insensitive.
//!
//! # Not supported (by design, M2b/M2c and Phase 4a Stage 0)
//!
//! - `--` / `/* */` comments: `-` is only ever a negative-number sign,
//!   so `-- comment` fails with "invalid number: -" rather than being
//!   skipped.
//! - Quoted identifiers (`"MyTable"`), hence no case-sensitive or
//!   keyword-colliding names.
//! - Multi-statement input (only ONE optional trailing `;`).
//! - RETURNING, JOIN, subqueries, aggregates, GROUP BY/HAVING, `<=`/`>=`/
//!   `<>`/`!=` (only `=` / `<` / `>`), parenthesized expressions, and general
//!   arithmetic outside the three ORDER BY distance operators.
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
    /// EXPLAIN of one inner statement (Stage A supports SELECT).
    Explain(Box<Statement>),
    CreateTable {
        name: String,
        columns: Vec<ColumnSpec>,
    },
    CreateSequence {
        name: String,
    },
    CreateGraph {
        name: String,
        directed: bool,
    },
    GraphAddNode {
        graph: String,
        node_id: Literal,
        properties: Literal,
    },
    GraphAddEdge {
        graph: String,
        source_node_id: Literal,
        target_node_id: Literal,
        properties: Literal,
    },
    GraphUpdateNode {
        graph: String,
        node_id: Literal,
        properties: Literal,
    },
    GraphDeleteNode {
        graph: String,
        node_id: Literal,
    },
    GraphDeleteEdge {
        graph: String,
        edge_id: Literal,
    },
    GraphBfs(GraphTraversal),
    GraphDfs(GraphTraversal),
    SequenceQuery {
        function: SequenceFunction,
        sequence: String,
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
        /// Optional second predicate introduced by `AND` (Phase 3 Stage 0).
        extra_filter: Option<Filter>,
        order_by: Option<OrderByKind>,
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
        /// Optional index name (Phase 4a: `CREATE INDEX name ON ...`).
        name: Option<String>,
        table: String,
        column: String,
        /// `USING <method>` — defaults to `BTree` (PG's `USING btree/gin/gist`
        /// family). `USING hnsw` requires an explicit opclass (§5.1).
        method: Option<IndexMethod>,
        /// The vector operator class (Phase 4a §5.1). Required for HNSW.
        opclass: Option<VectorOpclass>,
        /// Optional `WITH (fulltext_index = 'bm25')` declaration for inverted.
        fulltext_index: Option<String>,
    },
}

/// Direction of graph adjacency expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphDirection {
    Out,
    In,
    Both,
}

/// Shared BFS/DFS AST. Execution is introduced after the Stage 0 format freeze.
#[derive(Debug, Clone, PartialEq)]
pub struct GraphTraversal {
    pub graph: String,
    pub start_node_id: Literal,
    pub direction: GraphDirection,
    pub max_depth: u32,
}

/// A sequence function evaluated as a single-row, no-FROM query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceFunction {
    NextVal,
    CurrVal,
}

/// The index method (PG's `USING <method>` family).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexMethod {
    BTree,
    Hnsw,
    /// Inverted full-text index (Phase 3).
    Inverted,
}

/// The vector operator class (pgvector-compatible opclasses).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorOpclass {
    /// `vector_l2_ops` — L2 (Euclidean) distance.
    VectorL2Ops,
    /// `vector_cosine_ops` — cosine distance.
    VectorCosineOps,
    /// `vector_ip_ops` — inner product.
    VectorIpOps,
}

/// A distance-operator expression in ORDER BY (Phase 4a §5.2):
/// `ORDER BY <distance> ASC|DESC` where `<distance>` is one of the three
/// ANN operator shapes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceOp {
    /// `<->` — L2 (Euclidean) distance.
    L2,
    /// `<=>` — cosine distance.
    Cosine,
    /// `<#>` — inner product.
    InnerProduct,
}

/// ORDER BY clause: either a plain column or a distance-operator expression
/// (sum type — the two forms are mutually exclusive).
#[derive(Debug, Clone, PartialEq)]
pub enum OrderByKind {
    Column(OrderBy),
    Expr(OrderByExpr),
    /// BM25 full-text score expression (Phase 3).
    Bm25Score(Bm25ScoreExpr),
}

/// A distance-operator ORDER BY expression (Phase 4a §5.2).
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub op: DistanceOp,
    pub left: Expr,
    pub right: Expr,
    /// `true` = DESC, `false` = ASC (default).
    pub desc: bool,
}

/// `bm25_score(column, query_text)` used by SELECT lists and ORDER BY.
#[derive(Debug, Clone, PartialEq)]
pub struct Bm25ScoreExpr {
    /// Indexed text column to score.
    pub column: String,
    /// Full-text query text analyzed with the same analyzer as the index.
    pub query: Literal,
    /// Optional result-column label introduced by `AS`; not used for planning.
    pub alias: Option<String>,
    /// `true` = DESC, `false` = ASC (default).
    pub desc: bool,
}

/// An operand in an ORDER BY distance expression: a column reference or a
/// literal (e.g. the query vector).
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Literal(Literal),
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
    pub serial: bool,
}

/// Which columns a SELECT returns.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectCols {
    All,
    Cols(Vec<SelectItem>),
}

/// One SELECT-list item: a plain column or a vector distance expression.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    Column(String),
    /// Distance expression. `desc` is unused in a SELECT list and stays
    /// false; ORDER BY owns the direction for its own expression.
    Distance(OrderByExpr),
    /// BM25 full-text score expression (Phase 3).
    Bm25Score(Bm25ScoreExpr),
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
    /// `@@` full-text match (Phase 3).
    FullText,
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
    Float(f64),
    Str(String),
    Null,
    Default,
    /// M6 Stage C: a vector literal `[0.1, 2.5, ...]` (INSERT path).
    Vector(Vec<f32>),
    /// A 1-based bind parameter (`$1`, `$2`, ...).
    Param(usize),
}

// ─── Tokenizer ────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Token {
    // Keywords
    Begin,
    Commit,
    Rollback,
    Create,
    Explain,
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
    Sequence,
    Serial,
    Default,
    Null,
    Graph,
    Add,
    Node,
    Edge,
    Properties,
    Directed,
    Undirected,
    Bfs,
    Dfs,
    Out,
    In,
    Both,
    MaxDepth,
    // Type names
    Type(ColumnType),
    // Literals
    Int(i64),
    /// M6 Stage C: floating-point literal (vector elements).
    Float(f64),
    Str(String),
    Param(usize),
    // Operators
    Eq,
    Lt,
    Gt,
    /// `USING` (index method clause, Phase 4a).
    Using,
    /// `<->` — L2 distance operator (ANN, Phase 4a §5.2).
    L2Op,
    /// `<=>` — cosine distance operator (ANN).
    CosineOp,
    /// `<#>` — inner product operator (ANN).
    InnerProductOp,
    /// `@@` full-text match operator (Phase 3).
    FullTextOp,
    // Punctuation
    Comma,
    /// `[` — M6 Stage C vector literal open.
    LBracket,
    /// `]`.
    RBracket,
    Semicolon,
    LParen,
    RParen,
    Star,
    // Identifier (non-keyword)
    Ident(String),
    // End of input
    Eof,
}

const MAX_PARAMETER_COUNT: usize = u16::MAX as usize;

struct Tokenizer<'a> {
    chars: std::str::Chars<'a>,
    peek: Option<char>,
    token_index: usize,
    first_word: Option<String>,
    graph_statement: bool,
}

impl<'a> Tokenizer<'a> {
    fn new(input: &'a str) -> Self {
        let mut chars = input.chars();
        let peek = chars.next();
        Self {
            chars,
            peek,
            token_index: 0,
            first_word: None,
            graph_statement: false,
        }
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
                    } else if c == '$' {
                        tokens.push(self.parameter_placeholder()?);
                    } else if c == '-' || c.is_ascii_digit() {
                        tokens.push(self.number_literal()?);
                    } else if c == '@' {
                        self.bump();
                        if self.peek == Some('@') {
                            self.bump();
                            tokens.push(Token::FullTextOp);
                        } else {
                            return Err(EngineError::InvalidArgument(
                                "expected second '@' in full-text operator".to_string(),
                            ));
                        }
                    } else if c == '<' {
                        self.bump();
                        // Longest match first: the three ANN distance
                        // operators (`<->`, `<=>`, `<#>`) precede `<`.
                        // Look ahead for the complete operator before
                        // consuming its middle character. In particular,
                        // `<-5` is a less-than followed by a negative number,
                        // not a malformed `<->` operator.
                        let operator = match (self.peek, self.chars.clone().next()) {
                            (Some('-'), Some('>')) => Some(Token::L2Op),
                            (Some('='), Some('>')) => Some(Token::CosineOp),
                            (Some('#'), Some('>')) => Some(Token::InnerProductOp),
                            _ => None,
                        };
                        if let Some(operator) = operator {
                            self.bump();
                            self.bump();
                            tokens.push(operator);
                        } else {
                            tokens.push(Token::Lt);
                        }
                    } else if c == '=' {
                        self.bump();
                        tokens.push(Token::Eq);
                    } else if c == '>' {
                        self.bump();
                        tokens.push(Token::Gt);
                    } else if c == ',' {
                        self.bump();
                        tokens.push(Token::Comma);
                    } else if c == '[' {
                        self.bump();
                        tokens.push(Token::LBracket);
                    } else if c == ']' {
                        self.bump();
                        tokens.push(Token::RBracket);
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
                        tokens.push(self.identifier_or_keyword()?);
                    } else {
                        return Err(EngineError::InvalidArgument(format!(
                            "unexpected character {c:?} in SQL"
                        )));
                    }
                    self.token_index += 1;
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

    fn parameter_placeholder(&mut self) -> Result<Token> {
        self.bump(); // $
        let mut digits = String::new();
        while self.peek.is_some_and(|c| c.is_ascii_digit()) {
            digits.push(self.bump().unwrap());
        }
        if digits.is_empty() {
            return Err(EngineError::InvalidArgument(
                "malformed parameter placeholder: expected a decimal index after '$'".to_string(),
            ));
        }
        let parameter_limit_error = || {
            EngineError::InvalidArgument(format!(
                "parameter placeholder ${digits} exceeds the PostgreSQL parameter limit of {MAX_PARAMETER_COUNT}"
            ))
        };
        let index = digits
            .parse::<usize>()
            .map_err(|_| parameter_limit_error())?;
        if index == 0 {
            return Err(EngineError::InvalidArgument(
                "parameter placeholder indices are 1-based; $0 is invalid".to_string(),
            ));
        }
        if self
            .peek
            .is_some_and(|c| c.is_alphanumeric() || matches!(c, '_' | '.' | '$'))
        {
            return Err(EngineError::InvalidArgument(format!(
                "malformed parameter placeholder ${digits}: non-decimal suffix {:?}",
                self.peek.unwrap()
            )));
        }
        if index > MAX_PARAMETER_COUNT {
            return Err(parameter_limit_error());
        }
        Ok(Token::Param(index))
    }

    fn number_literal(&mut self) -> Result<Token> {
        let mut s = String::new();
        if self.peek == Some('-') {
            s.push(self.bump().unwrap());
        }
        let mut is_float = false;
        while let Some(c) = self.peek {
            if c.is_ascii_digit() {
                s.push(self.bump().unwrap());
            } else if c == '.' && !is_float && self.peek_next_is_digit() {
                // M6 Stage C: a single decimal point makes it a float
                // (vector elements). A second dot or trailing dot is a
                // parse error via the float conversion below.
                is_float = true;
                s.push(self.bump().unwrap());
            } else {
                break;
            }
        }
        if is_float {
            let v: f64 = s
                .parse()
                .map_err(|_| EngineError::InvalidArgument(format!("invalid number: {s}")))?;
            return Ok(Token::Float(v));
        }
        let n: i64 = s
            .parse()
            .map_err(|_| EngineError::InvalidArgument(format!("invalid number: {s}")))?;
        Ok(Token::Int(n))
    }

    /// Whether the character after the cursor is a digit (lookahead for
    /// the decimal-point rule).
    fn peek_next_is_digit(&self) -> bool {
        self.chars
            .clone()
            .next()
            .is_some_and(|c| c.is_ascii_digit())
    }

    fn identifier_or_keyword(&mut self) -> Result<Token> {
        let mut s = String::new();
        while let Some(c) = self.peek {
            if c.is_alphanumeric() || c == '_' {
                s.push(self.bump().unwrap());
            } else {
                break;
            }
        }
        let upper = s.to_ascii_uppercase();
        if self.first_word.is_none() {
            self.first_word = Some(upper.clone());
        }
        if upper == "GRAPH" && self.is_graph_statement_start() {
            self.graph_statement = true;
            return Ok(Token::Graph);
        }
        let graph = self.graph_statement;
        let token = match upper.as_str() {
            "BEGIN" => Token::Begin,
            "COMMIT" => Token::Commit,
            "ROLLBACK" => Token::Rollback,
            "CREATE" => Token::Create,
            "EXPLAIN" => Token::Explain,
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
            "USING" => Token::Using,
            "SEQUENCE" => Token::Sequence,
            "SERIAL" => Token::Serial,
            "DEFAULT" => Token::Default,
            "NULL" => Token::Null,
            "GRAPH" if graph => Token::Graph,
            "ADD" if graph => Token::Add,
            "NODE" if graph => Token::Node,
            "EDGE" if graph => Token::Edge,
            "PROPERTIES" if graph => Token::Properties,
            "DIRECTED" if graph => Token::Directed,
            "UNDIRECTED" if graph => Token::Undirected,
            "BFS" if graph => Token::Bfs,
            "DFS" if graph => Token::Dfs,
            "OUT" if graph => Token::Out,
            "IN" if graph => Token::In,
            "BOTH" if graph => Token::Both,
            "MAX_DEPTH" if graph => Token::MaxDepth,
            "INT" | "INT4" | "INTEGER" => Token::Type(ColumnType::Int4),
            "BIGINT" | "INT8" => Token::Type(ColumnType::Int8),
            "FLOAT8" => Token::Type(ColumnType::Float8),
            "TEXT" | "VARCHAR" => Token::Type(ColumnType::Text),
            "BYTEA" => Token::Type(ColumnType::Bytea),
            "JSONB" => Token::Type(ColumnType::Jsonb),
            "TIMESTAMPTZ" => Token::Type(ColumnType::Timestamptz),
            "UUID" => Token::Type(ColumnType::Uuid),
            // Unquoted identifiers fold to lowercase (PostgreSQL semantics;
            // this subset has no quoted identifiers).
            // M6 Stage C: `VECTOR(n)` declares a vector column of dim n.
            // Whitespace between the keyword and the parenthesis is
            // tolerated (PG's habit); a bare `VECTOR` stays a plain
            // identifier — nothing breaks if someone names a column
            // "vector".
            "VECTOR"
                if {
                    self.skip_whitespace();
                    self.peek == Some('(')
                } =>
            {
                self.bump(); // (
                let dim = match self.number_literal()? {
                    Token::Int(n) if (1..=2000).contains(&n) => n as u32,
                    other => {
                        return Err(EngineError::InvalidArgument(format!(
                            "VECTOR(n) requires an integer dim in 1..=2000, got {other:?}"
                        )))
                    }
                };
                match self.bump() {
                    Some(')') => {}
                    other => {
                        return Err(EngineError::InvalidArgument(format!(
                            "VECTOR(n): expected ')', got {other:?}"
                        )))
                    }
                }
                return Ok(Token::Type(ColumnType::Vector(dim)));
            }
            _ => Token::Ident(s.to_ascii_lowercase()),
        };
        Ok(token)
    }

    fn is_graph_statement_start(&self) -> bool {
        match self.token_index {
            0 => true,
            1 if matches!(self.first_word.as_deref(), Some("CREATE") | Some("EXPLAIN")) => {
                !self.next_non_whitespace_is_lparen()
            }
            _ => false,
        }
    }

    fn next_non_whitespace_is_lparen(&self) -> bool {
        self.peek
            .into_iter()
            .chain(self.chars.clone())
            .find(|character| !character.is_whitespace())
            == Some('(')
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
            Token::Graph => Ok("graph".to_string()),
            Token::Add => Ok("add".to_string()),
            Token::Node => Ok("node".to_string()),
            Token::Edge => Ok("edge".to_string()),
            Token::Properties => Ok("properties".to_string()),
            Token::Directed => Ok("directed".to_string()),
            Token::Undirected => Ok("undirected".to_string()),
            Token::Bfs => Ok("bfs".to_string()),
            Token::Dfs => Ok("dfs".to_string()),
            Token::Out => Ok("out".to_string()),
            Token::In => Ok("in".to_string()),
            Token::Both => Ok("both".to_string()),
            Token::MaxDepth => Ok("max_depth".to_string()),
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
            Token::Explain => {
                self.bump();
                Ok(Statement::Explain(Box::new(self.parse_statement()?)))
            }
            Token::Create => self.parse_create(),
            Token::Graph => self.parse_graph(),
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
            Token::Sequence => self.parse_create_sequence(),
            Token::Graph => self.parse_create_graph(),
            other => Err(EngineError::InvalidArgument(format!(
                "expected TABLE, INDEX, SEQUENCE or GRAPH after CREATE, got {other:?}"
            ))),
        }
    }

    fn parse_create_sequence(&mut self) -> Result<Statement> {
        self.bump(); // SEQUENCE
        let name = self.eat_ident()?;
        self.eat(&Token::Eof)?;
        Ok(Statement::CreateSequence { name })
    }

    fn parse_create_graph(&mut self) -> Result<Statement> {
        self.bump(); // GRAPH
        let name = self.eat_ident()?;
        let directed = match self.peek() {
            Token::Directed => {
                self.bump();
                true
            }
            Token::Undirected => {
                self.bump();
                false
            }
            Token::Eof => true,
            other => {
                return Err(EngineError::InvalidArgument(format!(
                    "expected DIRECTED, UNDIRECTED, or end of CREATE GRAPH, got {other:?}"
                )));
            }
        };
        self.eat(&Token::Eof)?;
        Ok(Statement::CreateGraph { name, directed })
    }

    fn parse_graph(&mut self) -> Result<Statement> {
        self.bump(); // GRAPH
        match self.peek().clone() {
            Token::Add => self.parse_graph_add(),
            Token::Update => self.parse_graph_update_node(),
            Token::Delete => self.parse_graph_delete(),
            Token::Bfs => self.parse_graph_traversal(true),
            Token::Dfs => self.parse_graph_traversal(false),
            other => Err(EngineError::InvalidArgument(format!(
                "expected ADD, UPDATE, DELETE, BFS, or DFS after GRAPH, got {other:?}"
            ))),
        }
    }

    fn parse_graph_add(&mut self) -> Result<Statement> {
        self.bump(); // ADD
        let node = match self.bump() {
            Token::Node => true,
            Token::Edge => false,
            other => {
                return Err(EngineError::InvalidArgument(format!(
                    "expected NODE or EDGE after GRAPH ADD, got {other:?}"
                )));
            }
        };
        let graph = self.eat_ident()?;
        let first = self.parse_graph_id()?;
        if node {
            let properties = self.parse_graph_properties()?;
            self.eat(&Token::Eof)?;
            Ok(Statement::GraphAddNode {
                graph,
                node_id: first,
                properties,
            })
        } else {
            let second = self.parse_graph_id()?;
            let properties = self.parse_graph_properties()?;
            self.eat(&Token::Eof)?;
            Ok(Statement::GraphAddEdge {
                graph,
                source_node_id: first,
                target_node_id: second,
                properties,
            })
        }
    }

    fn parse_graph_update_node(&mut self) -> Result<Statement> {
        self.bump(); // UPDATE
        self.eat(&Token::Node)?;
        let graph = self.eat_ident()?;
        let node_id = self.parse_graph_id()?;
        let properties = self.parse_graph_properties()?;
        self.eat(&Token::Eof)?;
        Ok(Statement::GraphUpdateNode {
            graph,
            node_id,
            properties,
        })
    }

    fn parse_graph_delete(&mut self) -> Result<Statement> {
        self.bump(); // DELETE
        let node = match self.bump() {
            Token::Node => true,
            Token::Edge => false,
            other => {
                return Err(EngineError::InvalidArgument(format!(
                    "expected NODE or EDGE after GRAPH DELETE, got {other:?}"
                )));
            }
        };
        let graph = self.eat_ident()?;
        let id = self.parse_graph_id()?;
        self.eat(&Token::Eof)?;
        if node {
            Ok(Statement::GraphDeleteNode { graph, node_id: id })
        } else {
            Ok(Statement::GraphDeleteEdge { graph, edge_id: id })
        }
    }

    fn parse_graph_traversal(&mut self, bfs: bool) -> Result<Statement> {
        self.bump(); // BFS or DFS
        let graph = self.eat_ident()?;
        let start_node_id = self.parse_graph_id()?;
        let direction = match self.bump() {
            Token::Out => GraphDirection::Out,
            Token::In => GraphDirection::In,
            Token::Both => GraphDirection::Both,
            other => {
                return Err(EngineError::InvalidArgument(format!(
                    "expected OUT, IN, or BOTH in graph traversal, got {other:?}"
                )));
            }
        };
        self.eat(&Token::MaxDepth)?;
        let max_depth = match self.bump() {
            Token::Int(depth) if (0..=3).contains(&depth) => depth as u32,
            other => {
                return Err(EngineError::InvalidArgument(format!(
                    "MAX_DEPTH requires an integer in 0..=3, got {other:?}"
                )));
            }
        };
        self.eat(&Token::Eof)?;
        let traversal = GraphTraversal {
            graph,
            start_node_id,
            direction,
            max_depth,
        };
        if bfs {
            Ok(Statement::GraphBfs(traversal))
        } else {
            Ok(Statement::GraphDfs(traversal))
        }
    }

    fn parse_graph_id(&mut self) -> Result<Literal> {
        let literal = self.parse_literal()?;
        if matches!(literal, Literal::Int(_) | Literal::Param(_)) {
            Ok(literal)
        } else {
            Err(EngineError::InvalidArgument(format!(
                "graph IDs require an INT8 literal or parameter, got {literal:?}"
            )))
        }
    }

    fn parse_graph_properties(&mut self) -> Result<Literal> {
        self.eat(&Token::Properties)?;
        let literal = self.parse_literal()?;
        match &literal {
            Literal::Str(text) => {
                serde_json::from_str::<serde_json::Value>(text).map_err(|error| {
                    EngineError::InvalidArgument(format!("invalid graph JSONB properties: {error}"))
                })?;
            }
            Literal::Param(_) => {}
            other => {
                return Err(EngineError::InvalidArgument(format!(
                    "graph PROPERTIES requires a JSONB text literal or parameter, got {other:?}"
                )));
            }
        }
        Ok(literal)
    }

    fn parse_create_table(&mut self) -> Result<Statement> {
        self.bump(); // TABLE
        let name = self.eat_ident()?;
        self.eat(&Token::LParen)?;
        let mut columns = Vec::new();
        loop {
            let col_name = self.eat_ident()?;
            let (col_type, serial) = match self.bump() {
                Token::Type(t) => (t, false),
                Token::Serial => (ColumnType::Int4, true),
                other => {
                    return Err(EngineError::InvalidArgument(format!(
                        "expected column type, got {other:?}"
                    )))
                }
            };
            columns.push(ColumnSpec {
                name: col_name,
                col_type,
                serial,
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
                     // Optional index name (Phase 4a: `CREATE INDEX name ON ...`).
        let name = if matches!(self.peek(), Token::Ident(_)) {
            Some(self.eat_ident()?)
        } else {
            None
        };
        self.eat(&Token::On)?;
        let table = self.eat_ident()?;
        // Optional `USING <method>` clause (defaults to BTree when absent).
        let method = if matches!(self.peek(), Token::Using) {
            self.bump(); // USING
            match self.eat_ident()?.as_str() {
                "btree" => Some(IndexMethod::BTree),
                "hnsw" => Some(IndexMethod::Hnsw),
                "inverted" => Some(IndexMethod::Inverted),
                other => {
                    return Err(EngineError::InvalidArgument(format!(
                        "unknown index method {other:?} (expected btree, hnsw, or inverted)"
                    )))
                }
            }
        } else {
            None
        };
        self.eat(&Token::LParen)?;
        let column = self.eat_ident()?;
        // Optional vector opclass (required for HNSW).
        let opclass = if matches!(self.peek(), Token::Ident(_)) {
            match self.eat_ident()?.as_str() {
                "vector_l2_ops" => Some(VectorOpclass::VectorL2Ops),
                "vector_cosine_ops" => Some(VectorOpclass::VectorCosineOps),
                "vector_ip_ops" => Some(VectorOpclass::VectorIpOps),
                other => {
                    return Err(EngineError::InvalidArgument(format!(
                        "unknown vector opclass {other:?} (expected vector_l2_ops/vector_cosine_ops/vector_ip_ops)"
                    )))
                }
            }
        } else {
            None
        };
        self.eat(&Token::RParen)?;
        let fulltext_index = self.parse_optional_fulltext_index()?;
        if method != Some(IndexMethod::Inverted) && fulltext_index.is_some() {
            return Err(EngineError::InvalidArgument(
                "fulltext_index is only valid with USING inverted".to_string(),
            ));
        }
        if method == Some(IndexMethod::Inverted) && opclass.is_some() {
            return Err(EngineError::InvalidArgument(
                "a vector opclass is not valid with USING inverted".to_string(),
            ));
        }
        // HNSW requires an explicit opclass (§5.1). Execution validates
        // method/column compatibility and rejects unsupported DDL forms.
        if method == Some(IndexMethod::Hnsw) && opclass.is_none() {
            return Err(EngineError::InvalidArgument(
                "HNSW index requires an opclass (vector_l2_ops/vector_cosine_ops/vector_ip_ops)"
                    .to_string(),
            ));
        }
        self.eat(&Token::Eof)?;
        Ok(Statement::CreateIndex {
            name,
            table,
            column,
            method,
            opclass,
            fulltext_index,
        })
    }

    fn parse_optional_fulltext_index(&mut self) -> Result<Option<String>> {
        if !matches!(self.peek(), Token::Ident(ref name) if name == "with") {
            return Ok(None);
        }
        self.bump(); // WITH
        self.eat(&Token::LParen)?;
        let option = self.eat_ident()?;
        if option != "fulltext_index" {
            return Err(EngineError::InvalidArgument(format!(
                "unknown index option {option:?}; expected fulltext_index"
            )));
        }
        self.eat(&Token::Eq)?;
        let value = match self.bump() {
            Token::Str(value) => value,
            other => {
                return Err(EngineError::InvalidArgument(format!(
                    "expected fulltext_index string value, got {other:?}"
                )))
            }
        };
        self.eat(&Token::RParen)?;
        if value != "bm25" {
            return Err(EngineError::Unsupported(format!(
                "unsupported fulltext_index value {value:?}; only 'bm25' is supported"
            )));
        }
        Ok(Some(value))
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
            Token::Float(f) => Ok(Literal::Float(f)),
            Token::Str(s) => Ok(Literal::Str(s)),
            Token::Default => Ok(Literal::Default),
            Token::Null => Ok(Literal::Null),
            Token::Param(index) => Ok(Literal::Param(index)),
            // M6 Stage C: `[1.0, 2.5, ...]` vector literal.
            Token::LBracket => {
                let mut v = Vec::new();
                if matches!(self.peek(), Token::RBracket) {
                    self.bump();
                    return Ok(Literal::Vector(v));
                }
                loop {
                    match self.bump() {
                        Token::Int(n) => v.push(n as f32),
                        Token::Float(f) => v.push(f as f32),
                        other => {
                            return Err(EngineError::InvalidArgument(format!(
                                "vector literal element must be a number, got {other:?}"
                            )))
                        }
                    }
                    match self.bump() {
                        Token::Comma => continue,
                        Token::RBracket => break,
                        other => {
                            return Err(EngineError::InvalidArgument(format!(
                                "vector literal: expected ',' or ']', got {other:?}"
                            )))
                        }
                    }
                }
                Ok(Literal::Vector(v))
            }
            other => Err(EngineError::InvalidArgument(format!(
                "expected literal, got {other:?}"
            ))),
        }
    }

    fn parse_select(&mut self) -> Result<Statement> {
        self.bump(); // SELECT
        if matches!(self.peek(), Token::Ident(name) if name == "nextval" || name == "currval")
            && matches!(self.tokens.get(self.pos + 1), Some(Token::LParen))
        {
            return self.parse_sequence_query();
        }
        let columns = self.parse_select_cols()?;
        self.eat(&Token::From)?;
        let table = self.eat_ident()?;
        let filter = self.parse_optional_filter()?;
        let extra_filter = self.parse_optional_extra_filter()?;
        let order_by = self.parse_optional_order_by()?;
        let limit = self.parse_optional_limit()?;
        let lock = self.parse_optional_lock_clause()?;
        self.eat(&Token::Eof)?;
        Ok(Statement::Select {
            columns,
            table,
            filter,
            extra_filter,
            order_by,
            limit,
            lock,
        })
    }

    fn parse_sequence_query(&mut self) -> Result<Statement> {
        let function = match self.bump() {
            Token::Ident(name) if name == "nextval" => SequenceFunction::NextVal,
            Token::Ident(name) if name == "currval" => SequenceFunction::CurrVal,
            other => {
                return Err(EngineError::InvalidArgument(format!(
                    "expected nextval or currval, got {other:?}"
                )))
            }
        };
        self.eat(&Token::LParen)?;
        let sequence = match self.bump() {
            Token::Str(name) => name,
            other => {
                return Err(EngineError::InvalidArgument(format!(
                    "sequence name must be a string literal, got {other:?}"
                )))
            }
        };
        self.eat(&Token::RParen)?;
        self.eat(&Token::Eof)?;
        Ok(Statement::SequenceQuery { function, sequence })
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
                let mut items = vec![self.parse_select_item()?];
                while matches!(self.peek(), Token::Comma) {
                    self.bump();
                    items.push(self.parse_select_item()?);
                }
                Ok(SelectCols::Cols(items))
            }
            other => Err(EngineError::InvalidArgument(format!(
                "expected column list or '*', got {other:?}"
            ))),
        }
    }

    fn parse_select_item(&mut self) -> Result<SelectItem> {
        let column = self.eat_ident()?;
        if column == "bm25_score" && matches!(self.peek(), Token::LParen) {
            self.eat(&Token::LParen)?;
            let column = self.eat_ident()?;
            self.eat(&Token::Comma)?;
            let query = self.parse_literal()?;
            self.eat(&Token::RParen)?;
            let alias = if matches!(self.peek(), Token::Ident(ref name) if name == "as") {
                self.bump();
                Some(self.eat_ident()?)
            } else {
                None
            };
            return Ok(SelectItem::Bm25Score(Bm25ScoreExpr {
                column,
                query,
                alias,
                desc: false,
            }));
        }
        if matches!(
            self.peek(),
            Token::L2Op | Token::CosineOp | Token::InnerProductOp
        ) {
            let (op, right) = self.parse_distance_tail()?;
            Ok(SelectItem::Distance(OrderByExpr {
                op,
                left: Expr::Column(column),
                right: Expr::Literal(right),
                desc: false,
            }))
        } else {
            Ok(SelectItem::Column(column))
        }
    }

    /// Parse the operator and right operand shared by SELECT-list and ORDER
    /// BY distance expressions. Direction belongs only to ORDER BY.
    fn parse_distance_tail(&mut self) -> Result<(DistanceOp, Literal)> {
        let op = match self.bump() {
            Token::L2Op => DistanceOp::L2,
            Token::CosineOp => DistanceOp::Cosine,
            Token::InnerProductOp => DistanceOp::InnerProduct,
            other => {
                return Err(EngineError::InvalidArgument(format!(
                    "expected vector distance operator, got {other:?}"
                )))
            }
        };
        Ok((op, self.parse_literal()?))
    }

    fn parse_optional_filter(&mut self) -> Result<Option<Filter>> {
        if !matches!(self.peek(), Token::Where) {
            return Ok(None);
        }
        self.bump(); // WHERE
        self.parse_filter().map(Some)
    }

    fn parse_optional_extra_filter(&mut self) -> Result<Option<Filter>> {
        if !matches!(self.peek(), Token::Ident(ref name) if name == "and") {
            return Ok(None);
        }
        self.bump(); // AND
        self.parse_filter().map(Some)
    }

    fn parse_filter(&mut self) -> Result<Filter> {
        let column = self.eat_ident()?;
        let op = match self.bump() {
            Token::Eq => CmpOp::Eq,
            Token::Lt => CmpOp::Lt,
            Token::Gt => CmpOp::Gt,
            Token::FullTextOp => CmpOp::FullText,
            other => {
                return Err(EngineError::InvalidArgument(format!(
                    "expected comparison operator, got {other:?}"
                )))
            }
        };
        let value = self.parse_literal()?;
        Ok(Filter { column, op, value })
    }

    fn parse_optional_order_by(&mut self) -> Result<Option<OrderByKind>> {
        if !matches!(self.peek(), Token::Order) {
            return Ok(None);
        }
        self.bump(); // ORDER
        self.eat(&Token::By)?;
        let column = self.eat_ident()?;
        let bm25_query = if column == "bm25_score" && matches!(self.peek(), Token::LParen) {
            self.eat(&Token::LParen)?;
            let score_column = self.eat_ident()?;
            self.eat(&Token::Comma)?;
            let query = self.parse_literal()?;
            self.eat(&Token::RParen)?;
            Some((score_column, query))
        } else {
            None
        };
        let distance = if bm25_query.is_none()
            && matches!(
                self.peek(),
                Token::L2Op | Token::CosineOp | Token::InnerProductOp
            ) {
            Some(self.parse_distance_tail()?)
        } else {
            None
        };
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
        if let Some((score_column, query)) = bm25_query {
            return Ok(Some(OrderByKind::Bm25Score(Bm25ScoreExpr {
                column: score_column,
                query,
                alias: None,
                desc,
            })));
        }
        if let Some((op, right)) = distance {
            Ok(Some(OrderByKind::Expr(OrderByExpr {
                op,
                left: Expr::Column(column),
                right: Expr::Literal(right),
                desc,
            })))
        } else {
            Ok(Some(OrderByKind::Column(OrderBy { column, desc })))
        }
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

pub(crate) fn max_parameter_index(stmt: &Statement) -> usize {
    fn literal_max(literal: &Literal) -> usize {
        match literal {
            Literal::Param(index) => *index,
            _ => 0,
        }
    }

    fn filter_max(filter: &Option<Filter>) -> usize {
        filter
            .as_ref()
            .map_or(0, |filter| literal_max(&filter.value))
    }

    fn expr_max(expr: &OrderByExpr) -> usize {
        let left = match &expr.left {
            Expr::Literal(literal) => literal_max(literal),
            Expr::Column(_) => 0,
        };
        let right = match &expr.right {
            Expr::Literal(literal) => literal_max(literal),
            Expr::Column(_) => 0,
        };
        left.max(right)
    }

    fn bm25_expr_max(expr: &Bm25ScoreExpr) -> usize {
        literal_max(&expr.query)
    }

    match stmt {
        Statement::Explain(inner) => max_parameter_index(inner),
        Statement::Insert { rows, .. } => rows.iter().flatten().map(literal_max).max().unwrap_or(0),
        Statement::Select {
            columns,
            filter,
            extra_filter,
            order_by,
            ..
        } => {
            let columns_max = match columns {
                SelectCols::All => 0,
                SelectCols::Cols(items) => items
                    .iter()
                    .map(|item| match item {
                        SelectItem::Column(_) => 0,
                        SelectItem::Distance(expr) => expr_max(expr),
                        SelectItem::Bm25Score(expr) => bm25_expr_max(expr),
                    })
                    .max()
                    .unwrap_or(0),
            };
            let order_max = match order_by {
                Some(OrderByKind::Expr(expr)) => expr_max(expr),
                Some(OrderByKind::Bm25Score(expr)) => bm25_expr_max(expr),
                _ => 0,
            };
            columns_max
                .max(filter_max(filter))
                .max(filter_max(extra_filter))
                .max(order_max)
        }
        Statement::Update { sets, filter, .. } => sets
            .iter()
            .map(|(_, literal)| literal_max(literal))
            .max()
            .unwrap_or(0)
            .max(filter_max(filter)),
        Statement::Delete { filter, .. } => filter_max(filter),
        Statement::GraphAddNode {
            node_id,
            properties,
            ..
        }
        | Statement::GraphUpdateNode {
            node_id,
            properties,
            ..
        } => literal_max(node_id).max(literal_max(properties)),
        Statement::GraphAddEdge {
            source_node_id,
            target_node_id,
            properties,
            ..
        } => literal_max(source_node_id)
            .max(literal_max(target_node_id))
            .max(literal_max(properties)),
        Statement::GraphDeleteNode { node_id, .. } => literal_max(node_id),
        Statement::GraphDeleteEdge { edge_id, .. } => literal_max(edge_id),
        Statement::GraphBfs(traversal) | Statement::GraphDfs(traversal) => {
            literal_max(&traversal.start_node_id)
        }
        Statement::Begin
        | Statement::Commit
        | Statement::Rollback
        | Statement::CreateTable { .. }
        | Statement::CreateIndex { .. }
        | Statement::CreateSequence { .. }
        | Statement::CreateGraph { .. }
        | Statement::SequenceQuery { .. } => 0,
    }
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
    fn parse_explain_select() {
        let stmt = parse("EXPLAIN SELECT * FROM users WHERE id = 1").unwrap();
        match stmt {
            Statement::Explain(inner) => {
                assert!(matches!(*inner, Statement::Select { .. }));
            }
            other => panic!("expected Explain, got {other:?}"),
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
                extra_filter: None,
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
                extra_filter: None,
                order_by,
                limit,
                lock,
            } => {
                assert_eq!(
                    columns,
                    SelectCols::Cols(vec![
                        SelectItem::Column("id".to_string()),
                        SelectItem::Column("name".to_string())
                    ])
                );
                assert_eq!(table, "users");
                let f = filter.unwrap();
                assert_eq!(f.column, "id");
                assert_eq!(f.op, CmpOp::Gt);
                assert_eq!(f.value, Literal::Int(10));
                let OrderByKind::Column(ob) = order_by.unwrap() else {
                    panic!("expected column order by");
                };
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
            Statement::CreateIndex { table, column, .. } => {
                assert_eq!(table, "users");
                assert_eq!(column, "email");
            }
            other => panic!("expected CreateIndex, got {other:?}"),
        }
    }

    #[test]
    fn parse_hnsw_create_index_with_all_opclasses() {
        for (text, expected) in [
            ("vector_l2_ops", VectorOpclass::VectorL2Ops),
            ("vector_cosine_ops", VectorOpclass::VectorCosineOps),
            ("vector_ip_ops", VectorOpclass::VectorIpOps),
        ] {
            let stmt = parse(&format!(
                "CREATE INDEX idx ON items USING hnsw (embedding {text})"
            ))
            .unwrap();
            match stmt {
                Statement::CreateIndex {
                    name,
                    table,
                    column,
                    method,
                    opclass,
                    ..
                } => {
                    assert_eq!(name.as_deref(), Some("idx"));
                    assert_eq!(table, "items");
                    assert_eq!(column, "embedding");
                    assert_eq!(method, Some(IndexMethod::Hnsw));
                    assert_eq!(opclass, Some(expected));
                }
                other => panic!("expected CreateIndex, got {other:?}"),
            }
        }
    }

    #[test]
    fn parse_create_index_rejects_invalid_hnsw_options() {
        assert!(parse("CREATE INDEX idx ON items USING hnsw (embedding)").is_err());
        assert!(parse("CREATE INDEX idx ON items USING hnsw (embedding vector_bad_ops)").is_err());
        assert!(parse("CREATE INDEX idx ON items USING gin (embedding)").is_err());
    }

    #[test]
    fn parse_distance_order_by_all_operators_and_directions() {
        for (text, expected) in [
            ("<->", DistanceOp::L2),
            ("<=>", DistanceOp::Cosine),
            ("<#>", DistanceOp::InnerProduct),
        ] {
            for (direction, desc) in [("ASC", false), ("DESC", true)] {
                let stmt = parse(&format!(
                    "SELECT * FROM items ORDER BY embedding {text} '[1,2,3]' {direction} LIMIT 4"
                ))
                .unwrap();
                match stmt {
                    Statement::Select {
                        order_by: Some(OrderByKind::Expr(expr)),
                        limit: Some(4),
                        ..
                    } => {
                        assert_eq!(expr.op, expected);
                        assert_eq!(expr.desc, desc);
                        assert_eq!(expr.left, Expr::Column("embedding".to_string()));
                        assert_eq!(
                            expr.right,
                            Expr::Literal(Literal::Str("[1,2,3]".to_string()))
                        );
                    }
                    other => panic!("expected distance SELECT, got {other:?}"),
                }
            }
        }
    }

    #[test]
    fn parse_select_list_mixes_columns_and_distance_expressions() {
        let stmt =
            parse("SELECT id, embedding <-> '[1,2,3]', embedding <#> [4,5,6] FROM items").unwrap();
        let Statement::Select { columns, .. } = stmt else {
            panic!("expected SELECT")
        };
        let SelectCols::Cols(items) = columns else {
            panic!("expected SELECT items")
        };
        assert_eq!(items.len(), 3);
        assert_eq!(items[0], SelectItem::Column("id".to_string()));
        let SelectItem::Distance(l2) = &items[1] else {
            panic!("expected L2 SELECT item")
        };
        assert_eq!(l2.op, DistanceOp::L2);
        assert!(!l2.desc);
        let SelectItem::Distance(ip) = &items[2] else {
            panic!("expected inner-product SELECT item")
        };
        assert_eq!(ip.op, DistanceOp::InnerProduct);
        assert_eq!(
            ip.right,
            Expr::Literal(Literal::Vector(vec![4.0, 5.0, 6.0]))
        );
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
    fn parse_negative_numbers_without_space_after_less_than() {
        let stmt = parse("SELECT * FROM t WHERE v<-5").unwrap();
        match stmt {
            Statement::Select {
                filter: Some(f), ..
            } => assert_eq!(f.value, Literal::Int(-5)),
            other => panic!("expected Select with filter, got {other:?}"),
        }
        assert!(parse("SELECT * FROM t WHERE v<-0.5").is_ok());
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
                assert_eq!(
                    columns,
                    SelectCols::Cols(vec![SelectItem::Column("id".to_string())])
                );
                assert_eq!(table, "users");
                assert_eq!(filter.unwrap().column, "id");
                let OrderByKind::Column(ob) = order_by.unwrap() else {
                    panic!("expected column order by");
                };
                assert_eq!(ob.column, "name");
            }
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn parse_inverted_index_and_validate_fulltext_option() {
        let stmt = parse(
            "CREATE INDEX content_idx ON agent_memory USING inverted (content) WITH (fulltext_index='bm25')",
        )
        .unwrap();
        assert_eq!(
            stmt,
            Statement::CreateIndex {
                name: Some("content_idx".to_string()),
                table: "agent_memory".to_string(),
                column: "content".to_string(),
                method: Some(IndexMethod::Inverted),
                opclass: None,
                fulltext_index: Some("bm25".to_string()),
            }
        );
        assert!(parse(
            "CREATE INDEX content_idx ON agent_memory USING inverted (content) WITH (fulltext_index='other')"
        )
        .is_err());
        assert!(parse(
            "CREATE INDEX content_idx ON agent_memory USING inverted (content) WITH (unknown='bm25')"
        )
        .is_err());
        assert!(parse(
            "CREATE INDEX content_idx ON agent_memory USING btree (content) WITH (fulltext_index='bm25')"
        )
        .is_err());
        assert!(parse(
            "CREATE INDEX content_idx ON agent_memory USING inverted (content vector_l2_ops)"
        )
        .is_err());
        assert!(parse("CREATE INDEX idx ON agent_memory USING gin (content)").is_err());
    }

    #[test]
    fn parse_fulltext_literal_parameter_and_extra_where() {
        let stmt = parse("SELECT * FROM agent_memory WHERE content @@ 'contract'").unwrap();
        let Statement::Select {
            filter: Some(filter),
            extra_filter: None,
            ..
        } = stmt
        else {
            panic!("expected full-text SELECT")
        };
        assert_eq!(filter.column, "content");
        assert_eq!(filter.op, CmpOp::FullText);
        assert_eq!(filter.value, Literal::Str("contract".to_string()));

        let stmt = parse("SELECT * FROM agent_memory WHERE content @@ $1").unwrap();
        assert_eq!(max_parameter_index(&stmt), 1);
        let Statement::Select {
            filter: Some(filter),
            ..
        } = stmt
        else {
            panic!("expected parameterized full-text SELECT")
        };
        assert_eq!(filter.op, CmpOp::FullText);
        assert_eq!(filter.value, Literal::Param(1));

        let stmt = parse("SELECT * FROM agent_memory WHERE content @@ 'x' AND id > 5").unwrap();
        let Statement::Select {
            filter: Some(filter),
            extra_filter: Some(extra),
            ..
        } = stmt
        else {
            panic!("expected ExtraWhere SELECT")
        };
        assert_eq!(filter.op, CmpOp::FullText);
        assert_eq!(extra.column, "id");
        assert_eq!(extra.op, CmpOp::Gt);
        assert_eq!(extra.value, Literal::Int(5));
    }

    #[test]
    fn bm25_score_identifier_remains_a_plain_column() {
        let stmt = parse("SELECT bm25_score FROM t").unwrap();
        let Statement::Select { columns, .. } = stmt else {
            panic!("expected SELECT")
        };
        assert_eq!(
            columns,
            SelectCols::Cols(vec![SelectItem::Column("bm25_score".to_string())])
        );

        let stmt = parse("SELECT * FROM t ORDER BY bm25_score").unwrap();
        let Statement::Select { order_by, .. } = stmt else {
            panic!("expected ORDER BY SELECT")
        };
        assert!(matches!(
            order_by,
            Some(OrderByKind::Column(OrderBy {
                column,
                desc: false,
            })) if column == "bm25_score"
        ));
    }

    #[test]
    fn parse_bm25_projection_order_and_alias() {
        let stmt = parse(
            "SELECT id, bm25_score(content, 'contract') AS score FROM agent_memory WHERE content @@ 'contract' ORDER BY bm25_score(content, 'contract') DESC LIMIT 10",
        )
        .unwrap();
        let Statement::Select {
            columns,
            order_by: Some(OrderByKind::Bm25Score(order)),
            limit: Some(10),
            ..
        } = stmt
        else {
            panic!("expected BM25 SELECT")
        };
        let SelectCols::Cols(items) = columns else {
            panic!("expected explicit projection")
        };
        assert_eq!(items.len(), 2);
        let SelectItem::Bm25Score(projection) = &items[1] else {
            panic!("expected BM25 projection")
        };
        assert_eq!(projection.column, "content");
        assert_eq!(projection.query, Literal::Str("contract".to_string()));
        assert_eq!(projection.alias.as_deref(), Some("score"));
        assert!(!projection.desc);
        assert_eq!(order.column, "content");
        assert_eq!(order.query, projection.query);
        assert!(order.desc);
        assert!(order.alias.is_none());
    }

    #[test]
    fn graph_ast_freezes_ddl_dml_traversal_and_jsonb() {
        assert_eq!(
            parse("CREATE GRAPH g").unwrap(),
            Statement::CreateGraph {
                name: "g".to_string(),
                directed: true,
            }
        );
        assert_eq!(
            parse("CREATE GRAPH g UNDIRECTED").unwrap(),
            Statement::CreateGraph {
                name: "g".to_string(),
                directed: false,
            }
        );
        for name in ["in", "out", "both", "node", "edge", "properties"] {
            assert_eq!(
                parse(&format!("CREATE GRAPH {name}")).unwrap(),
                Statement::CreateGraph {
                    name: name.to_string(),
                    directed: true,
                }
            );
        }
        assert!(matches!(
            parse("GRAPH ADD NODE g 1 PROPERTIES '{\"name\":\"A\"}'").unwrap(),
            Statement::GraphAddNode {
                node_id: Literal::Int(1),
                properties: Literal::Str(_),
                ..
            }
        ));
        let edge = parse("GRAPH ADD EDGE g $1 $2 PROPERTIES $3").unwrap();
        assert_eq!(max_parameter_index(&edge), 3);
        assert!(matches!(
            edge,
            Statement::GraphAddEdge {
                source_node_id: Literal::Param(1),
                target_node_id: Literal::Param(2),
                properties: Literal::Param(3),
                ..
            }
        ));
        assert!(matches!(
            parse("GRAPH UPDATE NODE g 1 PROPERTIES '{\"name\":\"B\"}'").unwrap(),
            Statement::GraphUpdateNode { .. }
        ));
        assert!(matches!(
            parse("GRAPH DELETE NODE g 1").unwrap(),
            Statement::GraphDeleteNode { .. }
        ));
        assert!(matches!(
            parse("GRAPH DELETE EDGE g 7").unwrap(),
            Statement::GraphDeleteEdge {
                edge_id: Literal::Int(7),
                ..
            }
        ));

        let Statement::GraphBfs(traversal) = parse("GRAPH BFS g 1 BOTH MAX_DEPTH 3").unwrap()
        else {
            panic!("expected GRAPH BFS")
        };
        assert_eq!(traversal.direction, GraphDirection::Both);
        assert_eq!(traversal.max_depth, 3);
        assert!(matches!(
            parse("GRAPH DFS g 1 IN MAX_DEPTH 0").unwrap(),
            Statement::GraphDfs(_)
        ));
        assert!(matches!(
            parse("EXPLAIN GRAPH BFS g 1 OUT MAX_DEPTH 2").unwrap(),
            Statement::Explain(inner) if matches!(*inner, Statement::GraphBfs(_))
        ));

        let Statement::CreateTable { columns, .. } =
            parse("CREATE TABLE events (payload JSONB)").unwrap()
        else {
            panic!("expected JSONB CREATE TABLE")
        };
        assert_eq!(columns[0].col_type, ColumnType::Jsonb);
    }

    #[test]
    fn graph_parser_rejects_invalid_depth_json_and_ids() {
        for sql in [
            "GRAPH BFS g 1 OUT MAX_DEPTH 4",
            "GRAPH BFS g 1 OUT MAX_DEPTH -1",
            "GRAPH ADD NODE g 1 PROPERTIES 'not json'",
            "GRAPH ADD NODE g 'not an id' PROPERTIES '{}'",
            "GRAPH ADD EDGE g 1 2 PROPERTIES NULL",
        ] {
            assert!(parse(sql).is_err(), "must reject: {sql}");
        }
    }

    #[test]
    fn graph_keywords_remain_plain_identifiers_outside_graph_statements() {
        let Statement::CreateTable { name, columns } =
            parse("CREATE TABLE graph (in INT, out TEXT, edge JSONB)").unwrap()
        else {
            panic!("expected CREATE TABLE with graph-like identifiers")
        };
        assert_eq!(name, "graph");
        assert_eq!(columns[0].name, "in");
        assert_eq!(columns[1].name, "out");
        assert_eq!(columns[2].name, "edge");

        let Statement::Select {
            columns: SelectCols::Cols(columns),
            ..
        } = parse(
            "SELECT graph, add, node, properties, directed, undirected, bfs, dfs, both, max_depth FROM t",
        )
        .unwrap()
        else {
            panic!("expected SELECT with graph-like column names")
        };
        assert_eq!(columns.len(), 10);
        for (index, name) in [
            "graph",
            "add",
            "node",
            "properties",
            "directed",
            "undirected",
            "bfs",
            "dfs",
            "both",
            "max_depth",
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(columns[index], SelectItem::Column(name.to_string()));
        }
        assert!(matches!(
            parse("SELECT in FROM t").unwrap(),
            Statement::Select {
                columns: SelectCols::Cols(_),
                ..
            }
        ));
    }
}
