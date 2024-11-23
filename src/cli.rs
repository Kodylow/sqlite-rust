use std::{env, fmt::Display, path::PathBuf};

/// Available commands for the SQLite CLI
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// Meta commands start with '.' (like .tables, .dbinfo)
    Meta(MetaCommand),
    /// SQL commands are any other valid SQL statements
    Sql(String),
}

/// Meta commands that start with '.'
#[derive(Debug, Clone, PartialEq)]
pub enum MetaCommand {
    DbInfo,
    Tables,
}

impl std::str::FromStr for Command {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with('.') {
            // Parse meta commands
            match s {
                ".dbinfo" => Ok(Command::Meta(MetaCommand::DbInfo)),
                ".tables" => Ok(Command::Meta(MetaCommand::Tables)),
                _ => Err(format!("Unknown meta command: {}", s)),
            }
        } else {
            // Treat everything else as SQL
            Ok(Command::Sql(s.to_string()))
        }
    }
}

impl Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Command::Meta(MetaCommand::DbInfo) => write!(f, ".dbinfo"),
            Command::Meta(MetaCommand::Tables) => write!(f, ".tables"),
            Command::Sql(sql) => write!(f, "{}", sql),
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
            return Err("Usage: <program> <database_file> <command-or-sql-statement>".to_string());
        }

        let file = PathBuf::from(&args[1]);
        let command = args[2].parse()?;

        Ok(Args { file, command })
    }
}
