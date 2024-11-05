use anyhow::Result;
use std::fs::File;
use std::io::prelude::*;

pub struct Database {
    file: File,
}

pub struct DatabaseInfo {
    pub page_size: u16,
    pub num_tables: u32,
}

impl Database {
    pub fn open(path: &str) -> Result<Self> {
        Ok(Self {
            file: File::open(path)?,
        })
    }

    pub fn get_info(&mut self) -> Result<DatabaseInfo> {
        let mut header = [0; 100];
        self.file.read_exact(&mut header)?;

        // Parse file header
        let page_size = u16::from_be_bytes([header[16], header[17]]);

        // Parse page header (starts at offset 100)
        let mut page_header = [0; 8]; // B-tree page header is 8 bytes
        self.file.read_exact(&mut page_header)?;

        // Number of cells is stored at offset 3-4 in the page header
        let num_tables = u16::from_be_bytes([page_header[3], page_header[4]]) as u32;

        Ok(DatabaseInfo {
            page_size,
            num_tables,
        })
    }
}
