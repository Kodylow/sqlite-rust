//! SQL Statement Execution
//!
//! This module handles execution of parsed SQL statements against a SQLite database.
//! It implements the logic to traverse B-tree pages and process records according
//! to the SQLite file format specification.

use anyhow::{anyhow, Result};
use std::fmt::Display;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use tracing::info;

use super::db::SQLiteDatabase;
use super::statement::{Expression, FunctionCall, Statement};

/// Result of executing a SQL statement
#[derive(Debug)]
pub enum ExecuteResult {
    /// Count result, used for COUNT(*) queries
    Count(u32),
    // Add other result types as needed
}

impl Display for ExecuteResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ExecuteResult::Count(count) => write!(f, "{}", count),
        }
    }
}

impl SQLiteDatabase {
    /// Executes a parsed SQL statement and returns the result
    pub fn execute(&mut self, stmt: &Statement) -> Result<ExecuteResult> {
        match &stmt.selections[0] {
            Expression::Function(FunctionCall { name, args }) => {
                if name.to_uppercase() == "COUNT" && args.len() == 1 {
                    if let Expression::Asterisk = args[0] {
                        return self.execute_count_all(&stmt.from_table);
                    }
                }
                Err(anyhow!("Unsupported function: {}", name))
            }
            _ => Err(anyhow!("Unsupported expression type")),
        }
    }

    /// Executes COUNT(*) by counting all records in a table
    fn execute_count_all(&mut self, table_name: &str) -> Result<ExecuteResult> {
        // First, find the root page for this table from sqlite_schema
        let root_page = self.find_table_root_page(table_name)?;

        // Count records starting from the root page
        let count = self.count_records_in_btree(root_page)?;

        Ok(ExecuteResult::Count(count))
    }

    /// Finds the root page number for a given table by reading sqlite_schema
    fn find_table_root_page(&mut self, table_name: &str) -> Result<u32> {
        info!("Finding root page for table: {}", table_name);
        let page_size = self.get_info()?.page_size() as usize;
        info!("Page size: {}", page_size);

        // Read first page which contains sqlite_schema
        let mut page = vec![0; page_size];
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut page)?;

        // Skip database header
        let header_size = 100;

        // Read B-tree page header
        let num_cells = u16::from_be_bytes([page[header_size + 3], page[header_size + 4]]);
        info!("Number of cells in sqlite_schema: {}", num_cells);

        // Read cell pointer array
        let mut cell_pointers = Vec::with_capacity(num_cells as usize);
        let array_start = header_size + 8;

        for i in 0..num_cells {
            let offset = array_start + (i as usize * 2);
            let ptr = u16::from_be_bytes([page[offset], page[offset + 1]]) as usize;
            cell_pointers.push(ptr);
        }
        info!("Cell pointers: {:?}", cell_pointers);

        // Process each cell looking for our table
        for (i, &ptr) in cell_pointers.iter().enumerate() {
            info!("Processing cell {}", i);
            let mut pos = ptr;

            // Skip payload length
            pos += self.varint_size(&page[pos..]);
            info!("After payload length, pos: {}", pos);

            // Skip rowid
            pos += self.varint_size(&page[pos..]);
            info!("After rowid, pos: {}", pos);

            // Read header size
            let header_size = self.read_varint(&page[pos..])? as usize;
            pos += self.varint_size(&page[pos..]);
            let header_end = pos + header_size - self.varint_size(&page[pos - 1..]);
            info!(
                "Header size: {}, pos: {}, header_end: {}",
                header_size, pos, header_end
            );

            // Read serial types
            let mut serial_types = Vec::new();
            while pos < header_end {
                let serial_type = self.read_varint(&page[pos..])?;
                pos += self.varint_size(&page[pos..]);
                serial_types.push(serial_type);
            }
            info!("Serial types: {:?}", serial_types);

            // Skip type field
            if let Some(&type_code) = serial_types.get(0) {
                if type_code >= 13 {
                    pos += ((type_code - 13) / 2) as usize;
                }
            }
            info!("After type field, pos: {}", pos);

            // Read table name
            if let Some(&name_type) = serial_types.get(2) {
                if name_type >= 13 {
                    let name_size = ((name_type - 13) / 2) as usize;
                    if let Ok(name) = String::from_utf8(page[pos..pos + name_size].to_vec()) {
                        info!("Found table name: {}", name);
                        if name == table_name {
                            info!("Found matching table!");

                            // Skip past table name and tbl_name fields
                            pos += name_size * 2; // Skip both name and tbl_name

                            // Now we're at the rootpage field
                            if let Some(&root_type) = serial_types.get(3) {
                                info!("Root page type: {}", root_type);
                                // Read the root page number based on its type
                                let root_page = match root_type {
                                    1 => page[pos] as u32,
                                    2 => u16::from_be_bytes([page[pos], page[pos + 1]]) as u32,
                                    3 => u32::from_be_bytes([
                                        0,
                                        page[pos],
                                        page[pos + 1],
                                        page[pos + 2],
                                    ]),
                                    4 => u32::from_be_bytes([
                                        page[pos],
                                        page[pos + 1],
                                        page[pos + 2],
                                        page[pos + 3],
                                    ]),
                                    _ => {
                                        return Err(anyhow!(
                                            "Invalid root page type: {}",
                                            root_type
                                        ))
                                    }
                                };
                                info!("Raw root page bytes: {:?}", &page[pos..pos + 4]);
                                info!("Found root page: {}", root_page);
                                return Ok(root_page);
                            }
                        }
                    }
                }
            }
        }

        Err(anyhow!("Table not found: {}", table_name))
    }

    /// Recursively counts records in a B-tree starting from given page
    fn count_records_in_btree(&mut self, page_num: u32) -> Result<u32> {
        info!("Counting records in page: {}", page_num);

        // Get page size with detailed logging
        let info = self.get_info()?;
        info!("DatabaseInfo raw: {:?}", info);
        let page_size = info.page_size();
        info!("Page size from info: {}", page_size);

        if page_size == 0 {
            // Log the current file position
            let pos = self.file.stream_position()?;
            info!("Current file position: {}", pos);

            // Try reading header bytes directly
            self.file.seek(SeekFrom::Start(16))?; // Page size is at offset 16
            let mut page_size_bytes = [0u8; 2];
            self.file.read_exact(&mut page_size_bytes)?;
            let direct_page_size = u16::from_be_bytes(page_size_bytes);
            info!(
                "Direct read page size bytes: {:?}, value: {}",
                page_size_bytes, direct_page_size
            );

            return Err(anyhow!("Invalid page size: 0"));
        }

        let mut page = vec![0; page_size as usize];

        // Calculate correct page offset - page numbers start at 1
        let offset = ((page_num - 1) as u64) * (page_size as u64);
        info!("Seeking to offset: {} for page {}", offset, page_num);

        // Verify file length
        let file_len = self.file.seek(SeekFrom::End(0))?;
        info!("File length: {}", file_len);
        if offset >= file_len {
            return Err(anyhow!(
                "Page offset {} exceeds file length {}",
                offset,
                file_len
            ));
        }

        // Read the page with detailed error checking
        self.file.seek(SeekFrom::Start(offset))?;
        let bytes_read = self.file.read(&mut page)?;
        info!("Read {} bytes at offset {}", bytes_read, offset);

        if bytes_read != page_size as usize {
            return Err(anyhow!(
                "Partial read: got {} bytes, expected {}",
                bytes_read,
                page_size
            ));
        }

        let page_type = page[0];
        info!("Read page type: {}", page_type);

        let num_cells = u16::from_be_bytes([page[3], page[4]]) as u32;
        info!("Number of cells: {}", num_cells);

        match page_type {
            13 => {
                info!("Leaf page, returning count: {}", num_cells);
                Ok(num_cells)
            }
            5 => {
                info!("Interior page, traversing children");
                let mut total = 0;
                let array_start = 12;

                for i in 0..num_cells {
                    let ptr_offset = array_start + (i as usize * 2);
                    let cell_ptr =
                        u16::from_be_bytes([page[ptr_offset], page[ptr_offset + 1]]) as usize;
                    info!("Processing child pointer at offset: {}", cell_ptr);

                    let child_page = u32::from_be_bytes([
                        page[cell_ptr],
                        page[cell_ptr + 1],
                        page[cell_ptr + 2],
                        page[cell_ptr + 3],
                    ]);
                    info!("Following child page: {}", child_page);

                    total += self.count_records_in_btree(child_page)?;
                }

                let rightmost = u32::from_be_bytes([page[8], page[9], page[10], page[11]]);
                info!("Processing rightmost page: {}", rightmost);
                total += self.count_records_in_btree(rightmost)?;

                info!("Total count for this subtree: {}", total);
                Ok(total)
            }
            _ => {
                info!("Invalid page type encountered: {}", page_type);
                Err(anyhow!("Invalid page type: {}", page_type))
            }
        }
    }
}
