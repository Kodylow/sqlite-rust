//! SQLite Record Format Implementation
//!
//! This module handles parsing SQLite records (rows) according to the file format specification.
//!
//! ## Record Format
//!
//! A record in SQLite represents a single row of data and consists of:
//!
//! - A header containing:
//!   - Total payload length (varint)
//!   - Row ID (varint) - only for table b-trees
//!   - Header size (varint)
//!   - Serial type codes (sequence of varints)
//! - The actual field data
//!
//! The serial type codes in the header describe the data type and size of each field:
//!
//! - 0: NULL
//! - 1: 8-bit signed int
//! - 2: 16-bit signed int
//! - 3: 24-bit signed int
//! - 4: 32-bit signed int
//! - 5: 48-bit signed int
//! - 6: 64-bit signed int
//! - 7: IEEE 754 64-bit float
//! - 8: 0 (legacy)
//! - 9: 1 (legacy)
//! - 10,11: Internal use
//! - N >= 13: Text/BLOB of (N-13)/2 bytes

use super::varint::Varint;
use anyhow::Result;

/// Parser for SQLite records (table/index rows)
pub struct Record<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> Record<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    pub fn skip_payload_length(&mut self) -> Result<()> {
        self.position += self.data.varint_size(&self.data[self.position..]);
        Ok(())
    }

    pub fn skip_rowid(&mut self) -> Result<()> {
        self.position += self.data.varint_size(&self.data[self.position..]);
        Ok(())
    }

    pub fn read_header(&mut self) -> Result<Vec<u64>> {
        let header_size = self.data.read_varint(&self.data[self.position..])? as usize;
        self.position += self.data.varint_size(&self.data[self.position..]);
        let header_end =
            self.position + header_size - self.data.varint_size(&self.data[self.position - 1..]);

        let mut serial_types = Vec::new();
        while self.position < header_end {
            let serial_type = self.data.read_varint(&self.data[self.position..])?;
            self.position += self.data.varint_size(&self.data[self.position..]);
            serial_types.push(serial_type);
        }

        Ok(serial_types)
    }

    pub fn skip_fields(&mut self, count: usize, serial_types: &[u64]) {
        for &type_code in serial_types.iter().take(count) {
            if type_code >= 13 {
                self.position += ((type_code - 13) / 2) as usize;
            }
        }
    }

    pub fn read_string_field(&self, type_code: u64) -> Result<Option<String>> {
        if type_code >= 13 {
            let size = ((type_code - 13) / 2) as usize;
            if let Ok(string) =
                String::from_utf8(self.data[self.position..self.position + size].to_vec())
            {
                return Ok(Some(string));
            }
        }
        Ok(None)
    }

    pub fn position(&self) -> usize {
        self.position
    }
}
