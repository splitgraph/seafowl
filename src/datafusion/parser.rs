// A copy of DataFusion's DFParser with one change:
// we want it to force sqlparser to not ignore CREATE FUNCTION.
// All code below is taken from https://github.com/apache/arrow-datafusion/blob/fe1995f31164774776baf67f6fb26ba66c42db0b/datafusion/sql/src/parser.rs
// with exception of our changes that are between the "XXX SEAFOWL:" blocks and no unit tests.
// --- DATAFUSION CODE BEGINS HERE ---

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! SQL Parser
//!
//! Declares a SQL parser based on sqlparser that handles custom formats that we need.

pub use datafusion::sql::parser::Statement;
use datafusion::sql::parser::{CopyToSource, CopyToStatement, CreateExternalTable};
use lazy_static::lazy_static;
use sqlparser::ast::{
    CreateFunctionBody, Expr, ObjectName, OrderByExpr, TruncateTableTarget, Value,
};
use sqlparser::tokenizer::{TokenWithLocation, Word};
use sqlparser::{
    ast::{ColumnDef, ColumnOptionDef, Statement as SQLStatement, TableConstraint},
    dialect::{keywords::Keyword, Dialect, GenericDialect},
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer},
};
use std::collections::VecDeque;

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr) => {
        Err(ParserError::ParserError($MSG.to_string()))
    };
}

fn parse_file_type(s: &str) -> Result<String, ParserError> {
    Ok(s.to_uppercase())
}

fn ensure_not_set<T>(field: &Option<T>, name: &str) -> Result<(), ParserError> {
    if field.is_some() {
        return Err(ParserError::ParserError(format!(
            "{name} specified more than once",
        )));
    }
    Ok(())
}

// XXX SEAFOWL: removed the struct definitions here because we want to use
// the original datafusion::sql::parser structs in order to pass them back
// to its logical planner

/// SQL Parser
pub struct DFParser<'a> {
    parser: Parser<'a>,
}

// Hacky way to distinguish `COPY TO` statement from `CONVERT TO DELTA`.
// We should really introduce our own Statement enum which encapsulates
// the DataFusion one and adds our custom variants (this one and `VACUUM`).
lazy_static! {
    pub static ref CONVERT_TO_DELTA: (String, Value) =
        ("CONVERT_TO_DELTA".to_string(), Value::Boolean(true));
}

impl<'a> DFParser<'a> {
    /// Parse the specified tokens
    pub fn new(sql: &str) -> Result<Self, ParserError> {
        let dialect = &GenericDialect {};
        DFParser::new_with_dialect(sql, dialect)
    }

    /// Parse the specified tokens with dialect
    pub fn new_with_dialect(
        sql: &str,
        dialect: &'a dyn Dialect,
    ) -> Result<Self, ParserError> {
        let mut tokenizer = Tokenizer::new(dialect, sql);
        let tokens = tokenizer.tokenize()?;

        Ok(DFParser {
            parser: Parser::new(dialect).with_tokens(tokens),
        })
    }

    /// Parse a SQL statement and produce a set of statements with dialect
    pub fn parse_sql(sql: &str) -> Result<VecDeque<Statement>, ParserError> {
        let dialect = &GenericDialect {};
        DFParser::parse_sql_with_dialect(sql, dialect)
    }

    /// Parse a SQL statement and produce a set of statements
    pub fn parse_sql_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<VecDeque<Statement>, ParserError> {
        let mut parser = DFParser::new_with_dialect(sql, dialect)?;
        let mut stmts = VecDeque::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while parser.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if parser.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return parser.expected("end of statement", parser.parser.peek_token());
            }

            let statement = parser.parse_statement()?;
            stmts.push_back(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Report an unexpected token
    fn expected<T>(
        &self,
        expected: &str,
        found: TokenWithLocation,
    ) -> Result<T, ParserError> {
        parser_err!(format!("Expected {expected}, found: {found}"))
    }

    /// Parse a new expression
    pub fn parse_statement(&mut self) -> Result<Statement, ParserError> {
        match self.parser.peek_token().token {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::CREATE => {
                        self.parser.next_token();
                        self.parse_create()
                    }
                    Keyword::CONVERT => {
                        self.parser.next_token();
                        self.parse_convert()
                    }
                    Keyword::COPY => {
                        self.parser.next_token();
                        self.parse_copy()
                    }
                    Keyword::VACUUM => {
                        self.parser.next_token();
                        self.parse_vacuum()
                    }
                    Keyword::TRUNCATE => {
                        self.parser.next_token();
                        self.parse_truncate()
                    }
                    _ => {
                        // use the native parser
                        Ok(Statement::Statement(Box::from(
                            self.parser.parse_statement()?,
                        )))
                    }
                }
            }
            _ => {
                // use the native parser
                Ok(Statement::Statement(Box::from(
                    self.parser.parse_statement()?,
                )))
            }
        }
    }

    // Parse `CONVERT location TO DELTA table_name` type statement
    pub fn parse_convert(&mut self) -> Result<Statement, ParserError> {
        let location = self.parser.parse_literal_string()?;
        self.parser
            .expect_keywords(&[Keyword::TO, Keyword::DELTA])?;
        let table_name = self.parser.parse_object_name(true)?;

        // We'll use the CopyToStatement struct to pass the location and table name
        // as it's the closest match to what we need.
        Ok(Statement::CopyTo(CopyToStatement {
            source: CopyToSource::Relation(table_name),
            target: location,
            options: vec![CONVERT_TO_DELTA.clone()],
            partitioned_by: vec![],
            stored_as: None,
        }))
    }

    pub fn parse_vacuum(&mut self) -> Result<Statement, ParserError> {
        // Since `VACUUM` is not a supported keyword by sqlparser, we abuse the semantically related
        // TRUNCATE to smuggle the info on whether we want GC of tables, partitions or the DB itself.
        let mut table_name = ObjectName(vec![]);
        let mut partitions = None;

        if self.parser.parse_keyword(Keyword::TABLE) {
            table_name = self.parser.parse_object_name(true)?;
        } else if self.parser.parse_keyword(Keyword::DATABASE) {
            let database_name = self.parser.parse_object_name(false)?.0[0].clone();
            partitions = Some(vec![Expr::Identifier(database_name)]);
        } else {
            return self.expected(
                "TABLE or DATABASE are supported VACUUM targets",
                self.parser.peek_token(),
            );
        }

        Ok(Statement::Statement(Box::new(SQLStatement::Truncate {
            table_names: vec![TruncateTableTarget { name: table_name }],
            partitions,
            table: false,
            only: false,
            identity: None,
            cascade: None,
        })))
    }

    pub fn parse_truncate(&mut self) -> Result<Statement, ParserError> {
        if !self.parser.parse_keyword(Keyword::TABLE) {
            return self.expected("TABLE as a TRUNCATE target", self.parser.peek_token());
        }

        let table_name = self.parser.parse_object_name(true)?;

        Ok(Statement::Statement(Box::new(SQLStatement::Truncate {
            table_names: vec![TruncateTableTarget { name: table_name }],
            partitions: None,
            table: true,
            only: false,
            identity: None,
            cascade: None,
        })))
    }

    /// Parse a SQL `COPY TO` statement
    pub fn parse_copy(&mut self) -> Result<Statement, ParserError> {
        // parse as a query
        let source = if self.parser.consume_token(&Token::LParen) {
            let query = self.parser.parse_query()?;
            self.parser.expect_token(&Token::RParen)?;
            CopyToSource::Query(query)
        } else {
            // parse as table reference
            let table_name = self.parser.parse_object_name(true)?;
            CopyToSource::Relation(table_name)
        };

        self.parser.expect_keyword(Keyword::TO)?;

        let target = self.parser.parse_literal_string()?;

        // check for options in parens
        let options = if self.parser.peek_token().token == Token::LParen {
            self.parse_value_options()?
        } else {
            vec![]
        };

        Ok(Statement::CopyTo(CopyToStatement {
            source,
            target,
            options,
            partitioned_by: vec![],
            stored_as: None,
        }))
    }

    /// Parse the next token as a key name for an option list
    ///
    /// Note this is different than [`parse_literal_string`]
    /// because it allows keywords as well as other non words
    ///
    /// [`parse_literal_string`]: sqlparser::parser::Parser::parse_literal_string
    pub fn parse_option_key(&mut self) -> Result<String, ParserError> {
        let next_token = self.parser.next_token();
        match next_token.token {
            Token::Word(Word { value, .. }) => {
                let mut parts = vec![value];
                while self.parser.consume_token(&Token::Period) {
                    let next_token = self.parser.next_token();
                    if let Token::Word(Word { value, .. }) = next_token.token {
                        parts.push(value);
                    } else {
                        // Unquoted namespaced keys have to conform to the syntax
                        // "<WORD>[\.<WORD>]*". If we have a key that breaks this
                        // pattern, error out:
                        return self.parser.expected("key name", next_token);
                    }
                }
                Ok(parts.join("."))
            }
            Token::SingleQuotedString(s) => Ok(s),
            Token::DoubleQuotedString(s) => Ok(s),
            Token::EscapedStringLiteral(s) => Ok(s),
            _ => self.parser.expected("key name", next_token),
        }
    }

    /// Parse the next token as a value for an option list
    ///
    /// Note this is different than [`parse_value`] as it allows any
    /// word or keyword in this location.
    ///
    /// [`parse_value`]: sqlparser::parser::Parser::parse_value
    pub fn parse_option_value(&mut self) -> Result<Value, ParserError> {
        let next_token = self.parser.next_token();
        match next_token.token {
            // e.g. things like "snappy" or "gzip" that may be keywords
            Token::Word(word) => Ok(Value::SingleQuotedString(word.value)),
            Token::SingleQuotedString(s) => Ok(Value::SingleQuotedString(s)),
            Token::DoubleQuotedString(s) => Ok(Value::DoubleQuotedString(s)),
            Token::EscapedStringLiteral(s) => Ok(Value::EscapedStringLiteral(s)),
            Token::Number(n, l) => Ok(Value::Number(n, l)),
            _ => self.parser.expected("string or numeric value", next_token),
        }
    }

    /// Parse a SQL CREATE statement
    pub fn parse_create(&mut self) -> Result<Statement, ParserError> {
        let or_replace = self.parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);

        if self.parser.parse_keyword(Keyword::EXTERNAL) {
            self.parse_create_external_table(false)
        } else if self.parser.parse_keyword(Keyword::UNBOUNDED) {
            self.parser.expect_keyword(Keyword::EXTERNAL)?;
            self.parse_create_external_table(true)
        }
        // XXX SEAFOWL: this is the change to get CREATE FUNCTION parsing working
        else if self.parser.parse_keyword(Keyword::FUNCTION) {
            // assume we don't have CREATE TEMPORARY FUNCTION (since we don't care about TEMPORARY)
            self.parse_create_function(or_replace, false)
        // XXX SEAFOWL: change ends here
        } else {
            Ok(Statement::Statement(Box::from(self.parser.parse_create()?)))
        }
    }

    /// Parse CREATE FUNCTION AS in the Hive dialect
    pub fn parse_create_function(
        &mut self,
        or_replace: bool,
        temporary: bool,
    ) -> Result<Statement, ParserError> {
        let name = self.parser.parse_object_name(false)?;
        self.parser.expect_keyword(Keyword::AS)?;
        let body = self.parse_create_function_body_string()?;

        let create_function = SQLStatement::CreateFunction {
            or_replace,
            temporary,
            if_not_exists: false,
            name,
            args: None,
            return_type: None,
            function_body: Some(CreateFunctionBody::AsBeforeOptions(body)),
            behavior: None,
            called_on_null: None,
            parallel: None,
            using: None,
            language: None,
            determinism_specifier: None,
            options: None,
            remote_connection: None,
        };

        Ok(Statement::Statement(Box::from(create_function)))
    }

    /// Parse the body of a `CREATE FUNCTION` specified as a string.
    /// e.g. `CREATE FUNCTION ... AS $$ body $$`.
    fn parse_create_function_body_string(&mut self) -> Result<Expr, ParserError> {
        let peek_token = self.parser.peek_token();
        match peek_token.token {
            Token::DollarQuotedString(s) => {
                self.parser.next_token();
                Ok(Expr::Value(Value::DollarQuotedString(s)))
            }
            _ => Ok(Expr::Value(Value::SingleQuotedString(
                self.parser.parse_literal_string()?,
            ))),
        }
    }

    fn parse_partitions(&mut self) -> Result<Vec<String>, ParserError> {
        let mut partitions: Vec<String> = vec![];
        if !self.parser.consume_token(&Token::LParen)
            || self.parser.consume_token(&Token::RParen)
        {
            return Ok(partitions);
        }

        loop {
            if let Token::Word(_) = self.parser.peek_token().token {
                let identifier = self.parser.parse_identifier(false)?;
                partitions.push(identifier.to_string());
            } else {
                return self.expected("partition name", self.parser.peek_token());
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after partition definition",
                    self.parser.peek_token(),
                );
            }
        }
        Ok(partitions)
    }

    /// Parse the ordering clause of a `CREATE EXTERNAL TABLE` SQL statement
    pub fn parse_order_by_exprs(&mut self) -> Result<Vec<OrderByExpr>, ParserError> {
        let mut values = vec![];
        self.parser.expect_token(&Token::LParen)?;
        loop {
            values.push(self.parse_order_by_expr()?);
            if !self.parser.consume_token(&Token::Comma) {
                self.parser.expect_token(&Token::RParen)?;
                return Ok(values);
            }
        }
    }

    /// Parse an ORDER BY sub-expression optionally followed by ASC or DESC.
    pub fn parse_order_by_expr(&mut self) -> Result<OrderByExpr, ParserError> {
        let expr = self.parser.parse_expr()?;

        let asc = if self.parser.parse_keyword(Keyword::ASC) {
            Some(true)
        } else if self.parser.parse_keyword(Keyword::DESC) {
            Some(false)
        } else {
            None
        };

        let nulls_first = if self
            .parser
            .parse_keywords(&[Keyword::NULLS, Keyword::FIRST])
        {
            Some(true)
        } else if self.parser.parse_keywords(&[Keyword::NULLS, Keyword::LAST]) {
            Some(false)
        } else {
            None
        };

        Ok(OrderByExpr {
            expr,
            asc,
            nulls_first,
            with_fill: None,
        })
    }

    // This is a copy of the equivalent implementation in sqlparser.
    fn parse_columns(
        &mut self,
    ) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>), ParserError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.parser.consume_token(&Token::LParen)
            || self.parser.consume_token(&Token::RParen)
        {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parser.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.parser.peek_token().token {
                let column_def = self.parse_column_def()?;
                columns.push(column_def);
            } else {
                return self.expected(
                    "column name or constraint definition",
                    self.parser.peek_token(),
                );
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after column definition",
                    self.parser.peek_token(),
                );
            }
        }

        Ok((columns, constraints))
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, ParserError> {
        let name = self.parser.parse_identifier(false)?;
        let data_type = self.parser.parse_data_type()?;
        let collation = if self.parser.parse_keyword(Keyword::COLLATE) {
            Some(self.parser.parse_object_name(false)?)
        } else {
            None
        };
        let mut options = vec![];
        loop {
            if self.parser.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.parser.parse_identifier(false)?);
                if let Some(option) = self.parser.parse_optional_column_option()? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return self.expected(
                        "constraint details after CONSTRAINT <name>",
                        self.parser.peek_token(),
                    );
                }
            } else if let Some(option) = self.parser.parse_optional_column_option()? {
                options.push(ColumnOptionDef { name: None, option });
            } else {
                break;
            };
        }
        Ok(ColumnDef {
            name,
            data_type,
            collation,
            options,
        })
    }

    fn parse_create_external_table(
        &mut self,
        unbounded: bool,
    ) -> Result<Statement, ParserError> {
        self.parser.expect_keyword(Keyword::TABLE)?;
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
        let table_name = self.parser.parse_object_name(true)?;
        let (mut columns, constraints) = self.parse_columns()?;

        #[derive(Default)]
        struct Builder {
            file_type: Option<String>,
            location: Option<String>,
            table_partition_cols: Option<Vec<String>>,
            order_exprs: Vec<Vec<OrderByExpr>>,
            options: Option<Vec<(String, Value)>>,
        }
        let mut builder = Builder::default();

        loop {
            if let Some(keyword) = self.parser.parse_one_of_keywords(&[
                Keyword::STORED,
                Keyword::LOCATION,
                Keyword::WITH,
                Keyword::DELIMITER,
                Keyword::COMPRESSION,
                Keyword::PARTITIONED,
                Keyword::OPTIONS,
            ]) {
                match keyword {
                    Keyword::STORED => {
                        self.parser.expect_keyword(Keyword::AS)?;
                        ensure_not_set(&builder.file_type, "STORED AS")?;
                        builder.file_type = Some(self.parse_file_format()?);
                    }
                    Keyword::LOCATION => {
                        ensure_not_set(&builder.location, "LOCATION")?;
                        builder.location = Some(self.parser.parse_literal_string()?);
                    }
                    Keyword::WITH => {
                        if self.parser.parse_keyword(Keyword::ORDER) {
                            builder.order_exprs.push(self.parse_order_by_exprs()?);
                        } else {
                            self.parser.expect_keyword(Keyword::HEADER)?;
                            self.parser.expect_keyword(Keyword::ROW)?;
                            return parser_err!("WITH HEADER ROW clause is no longer in use. Please use the OPTIONS clause with 'format.has_header' set appropriately, e.g., OPTIONS (format.has_header true)");
                        }
                    }
                    Keyword::DELIMITER => {
                        return parser_err!("DELIMITER clause is no longer in use. Please use the OPTIONS clause with 'format.delimiter' set appropriately, e.g., OPTIONS (format.delimiter ',')");
                    }
                    Keyword::COMPRESSION => {
                        self.parser.expect_keyword(Keyword::TYPE)?;
                        return parser_err!("COMPRESSION TYPE clause is no longer in use. Please use the OPTIONS clause with 'format.compression' set appropriately, e.g., OPTIONS (format.compression gzip)");
                    }
                    Keyword::PARTITIONED => {
                        self.parser.expect_keyword(Keyword::BY)?;
                        ensure_not_set(&builder.table_partition_cols, "PARTITIONED BY")?;
                        // Expects either list of column names (col_name [, col_name]*)
                        // or list of column definitions (col_name datatype [, col_name datatype]* )
                        // use the token after the name to decide which parsing rule to use
                        // Note that mixing both names and definitions is not allowed
                        let peeked = self.parser.peek_nth_token(2);
                        if peeked == Token::Comma || peeked == Token::RParen {
                            // list of column names
                            builder.table_partition_cols = Some(self.parse_partitions()?)
                        } else {
                            // list of column defs
                            let (cols, cons) = self.parse_columns()?;
                            builder.table_partition_cols = Some(
                                cols.iter().map(|col| col.name.to_string()).collect(),
                            );

                            columns.extend(cols);

                            if !cons.is_empty() {
                                return Err(ParserError::ParserError(
                                    "Constraints on Partition Columns are not supported"
                                        .to_string(),
                                ));
                            }
                        }
                    }
                    Keyword::OPTIONS => {
                        ensure_not_set(&builder.options, "OPTIONS")?;
                        builder.options = Some(self.parse_value_options()?);
                    }
                    _ => {
                        unreachable!()
                    }
                }
            } else {
                let token = self.parser.peek_token();
                if token == Token::EOF || token == Token::SemiColon {
                    break;
                } else {
                    return Err(ParserError::ParserError(format!(
                        "Unexpected token {token}"
                    )));
                }
            }
        }

        // Validations: location and file_type are required
        if builder.file_type.is_none() {
            return Err(ParserError::ParserError(
                "Missing STORED AS clause in CREATE EXTERNAL TABLE statement".into(),
            ));
        }
        if builder.location.is_none() {
            return Err(ParserError::ParserError(
                "Missing LOCATION clause in CREATE EXTERNAL TABLE statement".into(),
            ));
        }

        let create = CreateExternalTable {
            name: table_name,
            columns,
            file_type: builder.file_type.unwrap(),
            location: builder.location.unwrap(),
            table_partition_cols: builder.table_partition_cols.unwrap_or(vec![]),
            order_exprs: builder.order_exprs,
            if_not_exists,
            temporary: false,
            unbounded,
            options: builder.options.unwrap_or(Vec::new()),
            constraints,
        };
        Ok(Statement::CreateExternalTable(create))
    }

    /// Parses the set of valid formats
    fn parse_file_format(&mut self) -> Result<String, ParserError> {
        let token = self.parser.next_token();
        match &token.token {
            Token::Word(w) => parse_file_type(&w.value),
            _ => self.expected("one of ARROW, PARQUET, NDJSON, or CSV", token),
        }
    }

    fn parse_value_options(&mut self) -> Result<Vec<(String, Value)>, ParserError> {
        let mut options = vec![];
        self.parser.expect_token(&Token::LParen)?;

        loop {
            let key = self.parse_option_key()?;
            let value = self.parse_option_value()?;
            options.push((key, value));
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after option definition",
                    self.parser.peek_token(),
                );
            }
        }
        Ok(options)
    }
}
