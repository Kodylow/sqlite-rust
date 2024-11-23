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

use anyhow::{anyhow, Result};

/// Represents different types of SQL tokens
#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    /// Keywords in SQL (SELECT, FROM, etc)
    Keyword(String),
    /// Identifiers like table names, column names
    Identifier(String),
    /// Special characters and operators
    Symbol(char),
    /// Function names
    Function(String),
    /// The wildcard operator *
    Asterisk,
}

/// Represents a SQL function call
#[derive(Debug)]
pub struct FunctionCall {
    /// Name of the function (e.g., "COUNT")
    pub name: String,
    /// Arguments to the function
    pub args: Vec<Expression>,
}

/// Represents different types of SQL expressions
#[derive(Debug)]
pub enum Expression {
    /// A function call like COUNT(*)
    Function(FunctionCall),
    /// A wildcard selector *
    Asterisk,
    /// A column reference
    Column(String),
}

/// Represents a parsed SQL statement
#[derive(Debug)]
pub struct Statement {
    /// The expressions to select
    pub selections: Vec<Expression>,
    /// The table name to select from
    pub from_table: String,
}

impl Statement {
    /// Parses a SQL string into a Statement struct
    ///
    /// # Arguments
    ///
    /// * `sql` - The SQL string to parse
    ///
    /// # Returns
    ///
    /// A Result containing the parsed Statement or an error
    ///
    /// # Example
    ///
    /// ```
    /// let stmt = Statement::parse("SELECT COUNT(*) FROM apples")?;
    /// ```
    pub fn parse(sql: &str) -> Result<Self> {
        let tokens = Self::tokenize(sql)?;
        Self::parse_tokens(tokens)
    }

    /// Converts a SQL string into a vector of tokens
    ///
    /// # Example
    /// "SELECT COUNT(*)" becomes:
    /// [Keyword("SELECT"), Function("COUNT"), Symbol('('), Asterisk, Symbol(')')]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_count() -> Result<()> {
        let sql = "SELECT COUNT(*) FROM apples";
        let stmt = Statement::parse(sql)?;

        assert_eq!(stmt.from_table, "apples");
        assert_eq!(stmt.selections.len(), 1);

        if let Expression::Function(func) = &stmt.selections[0] {
            assert_eq!(func.name, "COUNT");
            assert_eq!(func.args.len(), 1);
            assert!(matches!(func.args[0], Expression::Asterisk));
        } else {
            panic!("Expected function expression");
        }

        Ok(())
    }
}
