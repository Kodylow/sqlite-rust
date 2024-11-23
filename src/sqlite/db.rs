//! SQLite File Format Implementation
//!
//! This module implements parsing of SQLite database files according to the SQLite file format specification.
//!
//! # SQLite File Structure
//!
//! A SQLite database file consists of one or more pages. The first page (page 1) contains:
//!
//! - Database header (100 bytes)
//! - First page of the sqlite_master table
use super::header::DatabaseHeader;
use super::table::TableReader;
use anyhow::Result;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;
use tracing::info;

/// Represents a SQLite database file
pub struct SQLiteDatabase {
    /// The underlying database file handle
    pub file: File,
    /// Parsed database header
    header: DatabaseHeader,
}

/// Contains metadata about a SQLite database
#[derive(Debug)]
pub struct SQLiteDatabaseInfo {
    /// Size of each page in bytes
    page_size: u16,
    /// Number of tables in the database
    num_tables: u32,
}

impl SQLiteDatabaseInfo {
    /// Returns the page size in bytes
    pub fn page_size(&self) -> u16 {
        self.page_size
    }

    /// Returns the number of tables in the database
    pub fn num_tables(&self) -> u32 {
        self.num_tables
    }
}

impl SQLiteDatabase {
    /// Opens a SQLite database file at the given path
    pub fn open(path: &PathBuf) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut header_bytes = vec![0; DatabaseHeader::HEADER_SIZE];
        file.read_exact(&mut header_bytes)?;

        let header = DatabaseHeader::parse(&header_bytes)?;

        Ok(Self { file, header })
    }

    /// Returns basic database information
    pub fn get_info(&mut self) -> Result<SQLiteDatabaseInfo> {
        let num_tables = self.list_tables()?.len() as u32;
        info!("Found {} tables", num_tables);

        Ok(SQLiteDatabaseInfo {
            page_size: self.header.page_size,
            num_tables,
        })
    }

    /// Lists all user tables in the database
    pub fn list_tables(&mut self) -> Result<Vec<String>> {
        let page_size = self.header.page_size as usize;
        let mut reader = TableReader::new(&mut self.file, page_size);
        reader.list_user_tables()
    }
}
