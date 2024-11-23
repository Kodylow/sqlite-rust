//! SQLite Database Header Implementation
//!
//! Handles parsing of the SQLite database header (first 100 bytes of the file)
//! according to the file format specification.
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

use anyhow::Result;
use tracing::info;

/// Represents the SQLite database header (first 100 bytes)
#[derive(Debug)]
pub struct DatabaseHeader {
    /// Page size in bytes (bytes 16-17)
    pub page_size: u16,
    /// File format write version (byte 18)
    pub write_version: u8,
    /// File format read version (byte 19)
    pub read_version: u8,
    /// Reserved space at end of each page (byte 20)
    pub reserved_space: u8,
    /// Maximum embedded payload fraction (byte 21)
    pub max_payload_fraction: u8,
    /// Minimum embedded payload fraction (byte 22)
    pub min_payload_fraction: u8,
    /// Leaf payload fraction (byte 23)
    pub leaf_payload_fraction: u8,
    /// File change counter (bytes 24-27)
    pub file_change_counter: u32,
    /// Size of database file in pages (bytes 28-31)
    pub database_size: u32,
    /// First freelist trunk page (bytes 32-35)
    pub first_freelist_trunk: u32,
    /// Total number of freelist pages (bytes 36-39)
    pub total_freelist_pages: u32,
    /// Schema cookie (bytes 40-43)
    pub schema_cookie: u32,
    /// Schema format number (bytes 44-47)
    pub schema_format: u32,
    /// Default page cache size (bytes 48-51)
    pub page_cache_size: u32,
    /// Largest root b-tree page number (bytes 52-55)
    pub largest_root_page: u32,
    /// Database text encoding (1:UTF-8, 2:UTF-16le, 3:UTF-16be) (bytes 56-59)
    pub text_encoding: u32,
    /// User version (bytes 60-63)
    pub user_version: u32,
    /// Incremental vacuum mode (bytes 64-67)
    pub incremental_vacuum: u32,
    /// Application ID (bytes 68-71)
    pub application_id: u32,
    /// Version valid for number (bytes 96-99)
    pub version_valid_for: u32,
    /// SQLite version number (bytes 92-95)
    pub sqlite_version_number: u32,
}

impl DatabaseHeader {
    /// Size of the SQLite database header in bytes
    pub const HEADER_SIZE: usize = 100;

    /// Magic string that should appear at the start of every SQLite file
    const MAGIC_STRING: &'static [u8] = b"SQLite format 3\0";

    /// Parses a database header from raw bytes
    pub fn parse(header_bytes: &[u8]) -> Result<Self> {
        if header_bytes.len() < Self::HEADER_SIZE {
            anyhow::bail!("Header buffer too small");
        }

        // Verify magic string
        if &header_bytes[0..16] != Self::MAGIC_STRING {
            anyhow::bail!("Invalid SQLite magic string");
        }

        let header = DatabaseHeader {
            page_size: u16::from_be_bytes([header_bytes[16], header_bytes[17]]),
            write_version: header_bytes[18],
            read_version: header_bytes[19],
            reserved_space: header_bytes[20],
            max_payload_fraction: header_bytes[21],
            min_payload_fraction: header_bytes[22],
            leaf_payload_fraction: header_bytes[23],
            file_change_counter: u32::from_be_bytes([
                header_bytes[24],
                header_bytes[25],
                header_bytes[26],
                header_bytes[27],
            ]),
            database_size: u32::from_be_bytes([
                header_bytes[28],
                header_bytes[29],
                header_bytes[30],
                header_bytes[31],
            ]),
            first_freelist_trunk: u32::from_be_bytes([
                header_bytes[32],
                header_bytes[33],
                header_bytes[34],
                header_bytes[35],
            ]),
            total_freelist_pages: u32::from_be_bytes([
                header_bytes[36],
                header_bytes[37],
                header_bytes[38],
                header_bytes[39],
            ]),
            schema_cookie: u32::from_be_bytes([
                header_bytes[40],
                header_bytes[41],
                header_bytes[42],
                header_bytes[43],
            ]),
            schema_format: u32::from_be_bytes([
                header_bytes[44],
                header_bytes[45],
                header_bytes[46],
                header_bytes[47],
            ]),
            page_cache_size: u32::from_be_bytes([
                header_bytes[48],
                header_bytes[49],
                header_bytes[50],
                header_bytes[51],
            ]),
            largest_root_page: u32::from_be_bytes([
                header_bytes[52],
                header_bytes[53],
                header_bytes[54],
                header_bytes[55],
            ]),
            text_encoding: u32::from_be_bytes([
                header_bytes[56],
                header_bytes[57],
                header_bytes[58],
                header_bytes[59],
            ]),
            user_version: u32::from_be_bytes([
                header_bytes[60],
                header_bytes[61],
                header_bytes[62],
                header_bytes[63],
            ]),
            incremental_vacuum: u32::from_be_bytes([
                header_bytes[64],
                header_bytes[65],
                header_bytes[66],
                header_bytes[67],
            ]),
            application_id: u32::from_be_bytes([
                header_bytes[68],
                header_bytes[69],
                header_bytes[70],
                header_bytes[71],
            ]),
            sqlite_version_number: u32::from_be_bytes([
                header_bytes[92],
                header_bytes[93],
                header_bytes[94],
                header_bytes[95],
            ]),
            version_valid_for: u32::from_be_bytes([
                header_bytes[96],
                header_bytes[97],
                header_bytes[98],
                header_bytes[99],
            ]),
        };

        info!("Parsed database header: {:?}", header);
        Ok(header)
    }

    /// Returns true if the database uses UTF-8 encoding
    pub fn is_utf8(&self) -> bool {
        self.text_encoding == 1
    }

    /// Returns true if the database uses UTF-16le encoding
    pub fn is_utf16le(&self) -> bool {
        self.text_encoding == 2
    }

    /// Returns true if the database uses UTF-16be encoding
    pub fn is_utf16be(&self) -> bool {
        self.text_encoding == 3
    }
}
