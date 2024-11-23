use anyhow::Result;
use tracing::info;
use tracing_subscriber::fmt;

pub mod backend;
pub mod cli;
pub mod frontend;

use crate::cli::{execute_command, Args};

fn main() -> Result<()> {
    fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse()?;
    info!("Starting sqlite-rs with {:?}", args);

    execute_command(args)?;

    Ok(())
}
