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

    /// Lists all user tables in the database
    ///
    /// Reads the sqlite_schema table from page 1 and extracts table names.
    /// Handles varint decoding and record format parsing according to SQLite spec.
    pub fn list_tables(&mut self) -> Result<Vec<String>> {
        let mut tables = Vec::new();
        let page_size = self.get_info()?.page_size() as usize;

        // Read first page
        let mut page = vec![0; page_size];
        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.read_exact(&mut page)?;

        // Skip database header
        let header_size = 100;

        // Read B-tree page header
        let num_cells = u16::from_be_bytes([page[header_size + 3], page[header_size + 4]]);
        let content_offset =
            u16::from_be_bytes([page[header_size + 5], page[header_size + 6]]) as usize;

        // Read cell pointer array
        let mut cell_pointers = Vec::with_capacity(num_cells as usize);
        let array_start = header_size + 8;

        for i in 0..num_cells {
            let offset = array_start + (i as usize * 2);
            let ptr = u16::from_be_bytes([page[offset], page[offset + 1]]) as usize;
            cell_pointers.push(ptr);
        }

        // Process each cell
        for &ptr in cell_pointers.iter() {
            let mut pos = ptr;

            // Skip payload length varint
            pos += self.varint_size(&page[pos..]);

            // Skip rowid varint
            pos += self.varint_size(&page[pos..]);

            // Read header size varint
            let header_size = self.read_varint(&page[pos..])? as usize;
            pos += self.varint_size(&page[pos..]);
            let header_end = pos + header_size - self.varint_size(&page[pos - 1..]);

            // Read serial types
            let mut serial_types = Vec::new();
            while pos < header_end {
                let serial_type = self.read_varint(&page[pos..])?;
                pos += self.varint_size(&page[pos..]);
                serial_types.push(serial_type);
            }

            // Skip type and name fields
            for i in 0..2 {
                let size = match serial_types[i] {
                    type_code if type_code >= 13 => (type_code - 13) / 2,
                    _ => continue,
                };
                pos += size as usize;
            }

            // Read table name
            if let Some(&tbl_name_type) = serial_types.get(2) {
                if tbl_name_type >= 13 {
                    let name_size = ((tbl_name_type - 13) / 2) as usize;
                    if let Ok(table_name) = String::from_utf8(page[pos..pos + name_size].to_vec()) {
                        if !table_name.starts_with("sqlite_") {
                            tables.push(table_name);
                        }
                    }
                }
            }
        }

        Ok(tables)
    }

    // Helper to read a varint
    fn read_varint(&self, bytes: &[u8]) -> Result<u64> {
        let mut result = 0u64;
        let mut shift = 0;

        for &byte in bytes.iter() {
            result |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }

        Ok(result)
    }

    // Helper to get varint size
    fn varint_size(&self, bytes: &[u8]) -> usize {
        let mut size = 0;
        while size < bytes.len() && bytes[size] & 0x80 != 0 {
            size += 1;
        }
        size + 1
    }
}
