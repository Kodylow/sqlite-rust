use anyhow::Result;
use clap::Parser;
use tracing_subscriber::fmt;

pub mod cli;
pub mod sqlite;

fn main() -> Result<()> {
    fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = cli::Args::parse();
    run(args)?;

    Ok(())
}

pub fn run(args: cli::Args) -> Result<()> {
    match args.command {
        cli::Command::DbInfo => {
            let mut db = sqlite::SQLiteDatabase::open(&args.file)?;
            let info = db.get_info()?;
            println!("database page size: {}", info.page_size());
            println!("number of tables: {}", info.num_tables());
        }
        cli::Command::Tables => {
            let mut db = sqlite::SQLiteDatabase::open(&args.file)?;
            let tables = db.list_tables()?;
            println!("{}", tables.join(" "));
        }
    }
    Ok(())
}
