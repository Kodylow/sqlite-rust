//! SQL Statement Execution
//!
//! This module handles execution of parsed SQL statements against a SQLite database.
//! It implements the logic to traverse B-tree pages and process records according
//! to the SQLite file format specification.

use super::btree::BTreePage;
use super::db::SQLiteDatabase;
use super::expression::{Expression, FunctionCall};
use super::record::Record;
use super::statement::Statement;
use crate::sqlite::varint::Varint;
use anyhow::{anyhow, Result};
use std::fmt::Display;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use tracing::info;

/// Result of executing a SQL statement
#[derive(Debug)]
pub enum ExecuteResult {
    /// Count result, used for COUNT(*) queries
    Count(u32),
    /// Values result, used for SELECT queries
    Values(Vec<String>),
}

impl Display for ExecuteResult {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ExecuteResult::Count(count) => write!(f, "{}", count),
            ExecuteResult::Values(values) => {
                for value in values {
                    writeln!(f, "{}", value)?;
                }
                Ok(())
            }
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
            Expression::Column(column_name) => self.read_column(&stmt.from_table, column_name),
            Expression::Asterisk => self.read_all_columns(&stmt.from_table),
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
            pos += page[pos..].varint_size(&page[pos..]);
            info!("After payload length, pos: {}", pos);

            // Skip rowid
            pos += page[pos..].varint_size(&page[pos..]);
            info!("After rowid, pos: {}", pos);

            // Read header size
            let header_size = page[pos..].read_varint(&page[pos..])? as usize;
            pos += page[pos..].varint_size(&page[pos..]);
            let header_end = pos + header_size - page[pos - 1..].varint_size(&page[pos - 1..]);
            info!(
                "Header size: {}, pos: {}, header_end: {}",
                header_size, pos, header_end
            );

            // Read serial types
            let mut serial_types = Vec::new();
            while pos < header_end {
                let serial_type = page[pos..].read_varint(&page[pos..])?;
                pos += page[pos..].varint_size(&page[pos..]);
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
        let page_size = self.get_info()?.page_size();

        let page = BTreePage::read(&mut self.file, page_num, page_size)?;

        match page.page_type() {
            13 => {
                // Leaf page
                info!("Leaf page, returning count: {}", page.num_cells());
                Ok(page.num_cells() as u32)
            }
            5 => {
                // Interior page
                info!("Interior page, traversing children");
                let mut total = 0;

                for child_page in page.get_child_pages()? {
                    info!("Following child page: {}", child_page);
                    total += self.count_records_in_btree(child_page)?;
                }

                info!("Total count for this subtree: {}", total);
                Ok(total)
            }
            pt => {
                info!("Invalid page type encountered: {}", pt);
                Err(anyhow!("Invalid page type: {}", pt))
            }
        }
    }

    /// Reads column values from a table
    fn read_column(&mut self, table_name: &str, column_name: &str) -> Result<ExecuteResult> {
        let root_page = self.find_table_root_page(table_name)?;
        let page_size = self.get_info()?.page_size();
        let mut values = Vec::new();

        // Read the root page
        let page = BTreePage::read(&mut self.file, root_page, page_size)?;

        // For now, assume it's a leaf page and just read the values
        // You'll need to handle interior pages later
        if page.page_type() == 13 {
            for i in 0..page.num_cells() {
                // This is a placeholder - you'll need to implement actual record reading
                values.push(format!("{}", i));
            }
        }

        Ok(ExecuteResult::Values(values))
    }

    /// Reads all columns from a table
    fn read_all_columns(&mut self, table_name: &str) -> Result<ExecuteResult> {
        let root_page = self.find_table_root_page(table_name)?;
        let page_size = self.get_info()?.page_size();

        let page = BTreePage::read(&mut self.file, root_page, page_size)?;
        let mut rows = Vec::new();

        // Read cells in reverse order since they're stored from end to start
        for i in (0..page.num_cells()).rev() {
            let cell_data = page.get_cell_data(i)?;
            let mut record = Record::new(&cell_data);

            // Read and skip the payload length
            let payload_length = record.read_varint()?;
            info!("Payload length: {}", payload_length);

            // Read and skip the rowid
            let rowid = record.read_varint()?;
            info!("Row ID: {}", rowid);

            // Read header
            let serial_types = record.read_header()?;
            info!("Serial types: {:?}", serial_types);

            let mut row = Vec::new();
            row.push(rowid.to_string()); // Add rowid as first column

            // Skip first serial type as it's for internal use
            for &type_code in serial_types.iter().skip(1) {
                let value = match type_code {
                    0 => "NULL".to_string(),
                    1..=6 => record.read_integer(type_code)?.to_string(),
                    7 => record.read_float()?.to_string(),
                    n if n >= 13 => {
                        if let Some(s) = record.read_string_field(type_code)? {
                            s
                        } else {
                            "NULL".to_string()
                        }
                    }
                    _ => "?".to_string(),
                };
                row.push(value);
            }

            rows.push(row.join("|"));
        }

        Ok(ExecuteResult::Values(rows))
    }
}
