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
//!
//! ## Database Header Format (First 100 bytes)
//!
//! - Bytes 0-15: Header string "SQLite format 3\0"
//! - Bytes 16-17: Page size in bytes (big-endian)
//! - Byte 18: File format write version
//! - Byte 19: File format read version  
//! - Byte 20: Reserved space at end of each page
//! - Bytes 21-23: Maximum embedded payload fraction, minimum embedded payload fraction, leaf payload fraction
//! - Byte 24: File change counter
//! - Bytes 28-31: Size of database file in pages
//! - Bytes 32-35: First freelist trunk page
//! - Bytes 36-39: Total number of freelist pages
//! - Bytes 40-43: Schema cookie
//! - Bytes 44-47: Schema format number
//! - Bytes 48-51: Default page cache size
//! - Bytes 52-55: Largest root b-tree page number
//! - Bytes 56-59: Database text encoding (1:UTF-8, 2:UTF-16le, 3:UTF-16be)
//! - Bytes 60-63: User version
//! - Bytes 64-67: Incremental vacuum mode
//! - Bytes 68-71: Application ID
//! - Bytes 72-95: Reserved for expansion
//! - Bytes 96-99: Version-valid-for number
//!
//! ## B-tree Page Structure
//!
//! Each page in the database file is a B-tree page that contains:
//!
//! - Page header (8-12 bytes)
//! - Cell pointer array
//! - Unallocated space
//! - Cell content area
//! - Reserved region
//!
//! ### B-tree Page Header Format
//!
//! - Byte 0: Page type
//! - Bytes 1-2: First freeblock offset
//! - Bytes 3-4: Number of cells
//! - Bytes 5-6: Cell content offset
//! - Byte 7: Number of fragmented free bytes

use anyhow::Result;
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;

/// Represents a SQLite database file
pub struct SQLiteDatabase {
    /// The underlying database file handle
    file: File,
}

/// Contains metadata about a SQLite database
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
        Ok(Self {
            file: File::open(path)?,
        })
    }

    /// Reads and parses the database header and first page to extract basic information
    pub fn get_info(&mut self) -> Result<SQLiteDatabaseInfo> {
        // Read database header (first 100 bytes)
        let mut header = [0; 100];
        self.file.read_exact(&mut header)?;

        // Parse page size from bytes 16-17 (big-endian)
        let page_size = u16::from_be_bytes([header[16], header[17]]);

        // Read B-tree page header that follows database header
        let mut page_header = [0; 8]; // B-tree page header is 8 bytes
        self.file.read_exact(&mut page_header)?;

        // Get number of cells (tables) from bytes 3-4 of page header
        let num_tables = u16::from_be_bytes([page_header[3], page_header[4]]) as u32;

        Ok(SQLiteDatabaseInfo {
            page_size,
            num_tables,
        })
    }
}
