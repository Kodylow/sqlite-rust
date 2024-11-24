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
