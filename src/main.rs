use anyhow::Result;
use tracing::info;
use tracing_subscriber::fmt;

pub mod cli;
pub mod sqlite;

fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "info");

    fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = cli::Args::parse().expect("Failed to parse arguments");
    run(args)?;

    Ok(())
}

pub fn run(args: cli::Args) -> Result<()> {
    match args.command {
        cli::Command::Meta(meta) => match meta {
            cli::MetaCommand::DbInfo => {
                let mut db = sqlite::db::SQLiteDatabase::open(&args.file)?;
                let info = db.get_info()?;
                println!("database page size: {}", info.page_size());
                println!("number of tables: {}", info.num_tables());
            }
            cli::MetaCommand::Tables => {
                let mut db = sqlite::db::SQLiteDatabase::open(&args.file)?;
                let tables = db.list_tables()?;
                println!("{}", tables.join(" "));
            }
        },
        // Try parsing SQL
        cli::Command::Sql(sql) => {
            let statement = sqlite::statement::Statement::parse(&sql)?;
            info!("Statement: {:?}", statement);
            let mut db = sqlite::db::SQLiteDatabase::open(&args.file)?;
            let result = db.execute(&statement)?;
            println!("{:?}", result);
        }
    }
    Ok(())
}
