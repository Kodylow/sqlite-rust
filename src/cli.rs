use std::{env, fmt::Display, path::PathBuf};

/// Available commands for the SQLite CLI
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    DbInfo,
    Tables,
}

impl std::str::FromStr for Command {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            ".dbinfo" => Ok(Command::DbInfo),
            ".tables" => Ok(Command::Tables),
            _ => Err(format!("Unknown command: {}", s)),
        }
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::DbInfo => write!(f, ".dbinfo"),
            Command::Tables => write!(f, ".tables"),
        }
    }
}

/// Command line arguments for the SQLite CLI
pub struct Args {
    /// Path to the SQLite database file to process
    pub file: PathBuf,

    /// The command to execute (dbinfo)
    pub command: Command,
}

impl Args {
    pub fn parse() -> Result<Self, String> {
        let args: Vec<String> = env::args().collect();

        if args.len() != 3 {
            return Err("Usage: <program> <database_file> <command>".to_string());
        }

        let file = PathBuf::from(&args[1]);
        let command = args[2].parse()?;

        Ok(Args { file, command })
    }
}
