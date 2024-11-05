use anyhow::Result;
use clap::Parser;
use std::fs::File;
use std::io::prelude::*;
use tracing_subscriber::fmt;

mod cli;

fn main() -> Result<()> {
    fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = cli::Args::parse();

    // Parse command and act accordingly
    match args.command {
        cli::Command::DbInfo => {
            let mut file = File::open(&args.file)?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
            #[allow(unused_variables)]
            let page_size = u16::from_be_bytes([header[16], header[17]]);

            // You can use print statements as follows for debugging, they'll be visible when running tests.
            println!("Logs from your program will appear here!");

            // Uncomment this block to pass the first stage
            println!("database page size: {}", page_size);
        }
    }

    Ok(())
}
