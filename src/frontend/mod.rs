//! The input to the front-end is a SQL query. The output is SQLite virtual machine bytecode (essentially a compiled program that can operate on the database).
pub mod code_gen;
pub mod parser;
pub mod tokenizer;
