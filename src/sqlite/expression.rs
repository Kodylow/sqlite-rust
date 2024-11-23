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
