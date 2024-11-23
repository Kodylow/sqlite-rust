use anyhow::{anyhow, Result};
use std::{
    env,
    fmt::Display,
    io::{self, Write},
    path::PathBuf,
};

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
#[derive(Debug)]
pub struct Args {
    pub file: PathBuf,
    pub command: Option<Command>,
}

impl Args {
    pub fn parse() -> Result<Self> {
        let args: Vec<String> = env::args().skip(1).collect();

        if args.is_empty() {
            // Start with in-memory database if no file specified
            return Ok(Args {
                file: PathBuf::from(":memory:"),
                command: None,
            });
        }

        let file = PathBuf::from(&args[0]);
        let command = args
            .get(1)
            .map(|s| s.parse::<Command>())
            .transpose()
            .map_err(|e| anyhow!(e))?;

        Ok(Args { file, command })
    }
}

pub struct InputBuffer {
    buffer: String,
}

impl InputBuffer {
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
        }
    }

    pub fn read_input(&mut self) -> Result<()> {
        self.buffer.clear();
        io::stdout().flush()?;
        io::stdin().read_line(&mut self.buffer)?;
        self.buffer = self.buffer.trim_end().to_string();
        Ok(())
    }
}

pub fn print_prompt() {
    print!("sqlite-rs> ");
}

pub fn handle_dbinfo() -> Result<()> {
    println!("Database info placeholder");
    Ok(())
}

pub fn handle_tables() -> Result<()> {
    println!("Tables placeholder");
    Ok(())
}

pub fn handle_command(command: &str) -> Result<bool> {
    match command {
        ".exit" => Ok(true),
        ".dbinfo" => {
            handle_dbinfo()?;
            Ok(false)
        }
        ".tables" => {
            handle_tables()?;
            Ok(false)
        }
        cmd if cmd.trim().is_empty() => Ok(false),
        _ => {
            println!("Unrecognized command '{}'.", command);
            Ok(false)
        }
    }
}

pub fn repl_mode() -> Result<()> {
    let mut input_buffer = InputBuffer::new();

    loop {
        print_prompt();
        input_buffer.read_input()?;

        if handle_command(&input_buffer.buffer)? {
            break Ok(());
        }
    }
}

pub fn execute_command(args: Args) -> Result<()> {
    match args.command {
        Some(Command::DbInfo) => handle_dbinfo(),
        Some(Command::Tables) => handle_tables(),
        None => repl_mode(),
    }
}
