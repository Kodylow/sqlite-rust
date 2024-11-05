use clap::{Parser, ValueEnum};
use std::{fmt::Display, path::PathBuf};

/// Available commands for the SQLite CLI
///
/// - `DbInfo`: Print the database page size
#[derive(ValueEnum, Debug, Clone, PartialEq)]
pub enum Command {
    #[value(name = ".dbinfo")]
    /// Print the database page size
    DbInfo,
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

/// Command line arguments for the SQLite CLI
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Path to the SQLite database file to process
    #[arg(value_name = "FILE")]
    pub file: PathBuf,

    /// The command to execute (dbinfo)
    #[arg(value_enum)]
    pub command: Command,
}
