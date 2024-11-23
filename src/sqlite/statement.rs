//! SQL Statement Parser
//!
//! This module implements parsing of SQL statements into structured representations.
//! It follows a two-step process:
//! 1. Lexical analysis (tokenization)
//! 2. Parsing tokens into a Statement AST
//!
//! # Example
//! ```
//! let sql = "SELECT COUNT(*) FROM apples";
//! let stmt = Statement::parse(sql)?;
//! ```

use crate::sqlite::expression::{Expression, FunctionCall};
use crate::sqlite::token::Token;
use anyhow::{anyhow, Result};

/// Represents a parsed SQL statement
#[derive(Debug)]
pub struct Statement {
    /// The expressions to select
    pub selections: Vec<Expression>,
    /// The table name to apply the selections to
    pub from_table: String,
}

impl Statement {
    /// Parses a SQL string into a Statement struct
    pub fn parse(sql: &str) -> Result<Self> {
        let tokens = Self::tokenize(sql)?;
        Self::parse_tokens(tokens)
    }

    /// Converts a SQL string into a vector of tokens
    fn tokenize(sql: &str) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        let mut chars = sql.chars().peekable();

        while let Some(&c) = chars.peek() {
            match c {
                // Skip whitespace
                c if c.is_whitespace() => {
                    chars.next();
                }

                // Handle identifiers and keywords
                c if c.is_alphabetic() => {
                    let mut word = String::new();
                    while let Some(&c) = chars.peek() {
                        if c.is_alphanumeric() {
                            word.push(c);
                            chars.next();
                        } else {
                            break;
                        }
                    }

                    let token = match word.to_uppercase().as_str() {
                        "SELECT" | "FROM" => Token::Keyword(word),
                        "COUNT" => Token::Function(word),
                        _ => Token::Identifier(word),
                    };
                    tokens.push(token);
                }

                // Handle special characters
                '*' => {
                    tokens.push(Token::Asterisk);
                    chars.next();
                }
                '(' | ')' => {
                    tokens.push(Token::Symbol(c));
                    chars.next();
                }

                _ => return Err(anyhow!("Unexpected character: {}", c)),
            }
        }

        Ok(tokens)
    }

    /// Parses a vector of tokens into a Statement struct
    fn parse_tokens(tokens: Vec<Token>) -> Result<Self> {
        let mut iter = tokens.into_iter().peekable();
        let mut selections = Vec::new();

        // Expect SELECT
        match iter.next() {
            Some(Token::Keyword(k)) if k.to_uppercase() == "SELECT" => {}
            _ => return Err(anyhow!("Expected SELECT keyword")),
        }

        // Parse selections
        while let Some(token) = iter.next() {
            match token {
                Token::Function(name) => {
                    // Handle function call
                    match iter.next() {
                        Some(Token::Symbol('(')) => {}
                        _ => return Err(anyhow!("Expected opening parenthesis after function")),
                    }

                    match iter.next() {
                        Some(Token::Asterisk) => {}
                        _ => return Err(anyhow!("Expected * in function argument")),
                    }

                    match iter.next() {
                        Some(Token::Symbol(')')) => {}
                        _ => return Err(anyhow!("Expected closing parenthesis")),
                    }

                    selections.push(Expression::Function(FunctionCall {
                        name,
                        args: vec![Expression::Asterisk],
                    }));
                }
                Token::Keyword(k) if k.to_uppercase() == "FROM" => break,
                _ => return Err(anyhow!("Unexpected token in selections")),
            }
        }

        // Parse FROM clause
        let from_table = match iter.next() {
            Some(Token::Identifier(table)) => table,
            _ => return Err(anyhow!("Expected table name after FROM")),
        };

        Ok(Statement {
            selections,
            from_table,
        })
    }
}
