use anyhow::{anyhow, Result};
use chrono::Local;
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

/// SQL Statement related types
#[derive(Debug)]
pub enum StatementType {
    Insert,
    Select,
}

pub struct Statement {
    statement_type: StatementType,
}

#[derive(Debug)]
pub enum PrepareResult {
    Success,
    UnrecognizedStatement,
}

#[derive(Debug)]
pub enum MetaCommandResult {
    Success,
    UnrecognizedCommand,
}

/// CLI Arguments handling
#[derive(Debug)]
pub struct Args {
    pub file: PathBuf,
    pub command: Option<Command>,
}

impl Args {
    pub fn parse() -> Result<Self> {
        let args: Vec<String> = env::args().skip(1).collect();

        if args.is_empty() {
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

/// Input handling
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

/// Statement handling
pub fn prepare_statement(input: &str) -> Result<(PrepareResult, Option<Statement>)> {
    if input.starts_with("insert") {
        Ok((
            PrepareResult::Success,
            Some(Statement {
                statement_type: StatementType::Insert,
            }),
        ))
    } else if input.starts_with("select") {
        Ok((
            PrepareResult::Success,
            Some(Statement {
                statement_type: StatementType::Select,
            }),
        ))
    } else {
        Ok((PrepareResult::UnrecognizedStatement, None))
    }
}

pub fn execute_statement(statement: &Statement) -> Result<()> {
    match statement.statement_type {
        StatementType::Insert => println!("This is where we would do an insert."),
        StatementType::Select => println!("This is where we would do a select."),
    }
    Ok(())
}

/// Command handling
pub fn do_meta_command(command: &str) -> MetaCommandResult {
    match command {
        ".exit" => std::process::exit(0),
        ".help" => handle_help(),
        ".dbinfo" => handle_dbinfo(),
        ".tables" => handle_tables(),
        _ => MetaCommandResult::UnrecognizedCommand,
    }
}

pub fn handle_command(command: &str) -> Result<bool> {
    if command.starts_with('.') {
        match do_meta_command(command) {
            MetaCommandResult::Success => Ok(false),
            MetaCommandResult::UnrecognizedCommand => {
                println!("Unrecognized command '{}'.", command);
                Ok(false)
            }
        }
    } else {
        let (prepare_result, statement) = prepare_statement(command)?;
        match prepare_result {
            PrepareResult::Success => {
                execute_statement(statement.as_ref().unwrap())?;
                Ok(false)
            }
            PrepareResult::UnrecognizedStatement => {
                println!("Unrecognized keyword at start of '{}'.", command);
                Ok(false)
            }
        }
    }
}

/// Command implementations
pub fn handle_dbinfo() -> MetaCommandResult {
    println!("Database info placeholder");
    MetaCommandResult::Success
}

pub fn handle_tables() -> MetaCommandResult {
    println!("Tables placeholder");
    MetaCommandResult::Success
}

pub fn handle_help() -> MetaCommandResult {
    println!("SQLite-rs v0.1.0");
    println!("Commands:");
    println!(".exit - Exit the program");
    println!(".help - Print this help message");
    println!(".dbinfo - Print database info");
    println!(".tables - Print table names");
    MetaCommandResult::Success
}

/// REPL functionality
pub fn print_prompt() {
    print!("sqlite-rs> ");
}

pub fn repl_mode() -> Result<()> {
    let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    println!("SQLite-rs v0.1.0 {}", timestamp);
    println!("Enter \".help\" for usage hints.");
    println!("Connected to a transient in-memory database.");
    println!("Use \".open FILENAME\" to reopen on a persistent database.");

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
        Some(Command::DbInfo) => {
            handle_dbinfo();
            Ok(())
        }
        Some(Command::Tables) => {
            handle_tables();
            Ok(())
        }
        None => repl_mode(),
    }
}
