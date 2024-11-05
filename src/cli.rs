use clap::{AppSettings, Parser};
use std::{fmt::Display, path::PathBuf};

/// Available commands for the SQLite CLI
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    DbInfo,
}

impl std::str::FromStr for Command {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            ".dbinfo" => Ok(Command::DbInfo),
            _ => Err(format!("Unknown command: {}", s)),
        }
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::DbInfo => write!(f, ".dbinfo"),
        }
    }
}

/// Command line arguments for the SQLite CLI
#[derive(Parser, Debug)]
#[clap(version, author, about)]
#[clap(setting = AppSettings::ColoredHelp)]
pub struct Args {
    /// Path to the SQLite database file to process
    pub file: PathBuf,

    /// The command to execute (dbinfo)
    pub command: Command,
}
