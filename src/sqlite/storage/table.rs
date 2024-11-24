use crate::sqlite::core::btree::BTreePageHeader;
use crate::sqlite::core::header::DatabaseHeader;
use crate::sqlite::core::record::Record;
use crate::sqlite::core::schema::TableSchema;
use anyhow::{anyhow, Result};
use std::fs::File;
use std::io::{prelude::*, SeekFrom};
use tracing::info;

pub struct TableReader<'a> {
    file: &'a mut File,
    page_size: usize,
}

impl<'a> TableReader<'a> {
    pub fn new(file: &'a mut File, page_size: usize) -> Self {
        Self { file, page_size }
    }

    pub fn list_user_tables(&mut self) -> Result<Vec<String>> {
        let mut tables = Vec::new();

        // Read first page
        let mut page = vec![0; self.page_size];
        self.file.seek(std::io::SeekFrom::Start(0))?;
        self.file.read_exact(&mut page)?;

        // Skip database header and read B-tree page header
        let btree_header = BTreePageHeader::parse(&page[DatabaseHeader::HEADER_SIZE..])?;

        // Read cell pointer array
        let cell_pointers =
            self.read_cell_pointers(&page, btree_header, DatabaseHeader::HEADER_SIZE);

        // Process each cell
        for &ptr in cell_pointers.iter() {
            if let Some(table_name) = self.read_table_name(&page, ptr)? {
                if !table_name.starts_with("sqlite_") {
                    tables.push(table_name);
                }
            }
        }

        Ok(tables)
    }

    fn read_cell_pointers(
        &self,
        page: &[u8],
        header: BTreePageHeader,
        header_offset: usize,
    ) -> Vec<usize> {
        let mut pointers = Vec::with_capacity(header.num_cells as usize);
        let array_start = header_offset + 8; // Skip page header

        for i in 0..header.num_cells {
            let offset = array_start + (i as usize * 2);
            let ptr = u16::from_be_bytes([page[offset], page[offset + 1]]) as usize;
            pointers.push(ptr);
        }

        pointers
    }

    fn read_table_name(&self, page: &[u8], ptr: usize) -> Result<Option<String>> {
        let mut record = Record::new(&page[ptr..]);

        record.skip_payload_length()?;
        record.skip_rowid()?;

        let serial_types = record.read_header()?;
        record.skip_fields(2, &serial_types); // Skip type and name fields

        if let Some(&tbl_name_type) = serial_types.get(2) {
            return record.read_string_field(tbl_name_type);
        }

        Ok(None)
    }

    pub fn get_table_schema(&mut self, table_name: &str) -> Result<TableSchema> {
        // Read first page containing sqlite_schema
        let mut page = vec![0; self.page_size];
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut page)?;

        // Skip database header
        let header_size = DatabaseHeader::HEADER_SIZE;

        // Read B-tree page header
        let btree_header = BTreePageHeader::parse(&page[header_size..])?;
        let num_cells = btree_header.num_cells;
        info!("Number of cells in sqlite_schema: {}", num_cells);

        // Process cells looking for our table
        for i in 0..num_cells {
            let cell_data = self.read_cell(&page, i as usize, header_size)?;
            let mut record = Record::new(&cell_data);

            // Skip payload length and rowid
            let payload_length = record.read_varint()?;
            let rowid = record.read_varint()?;
            info!(
                "Processing record - payload_length: {}, rowid: {}",
                payload_length, rowid
            );

            // Read header
            let serial_types = record.read_header()?;
            info!(
                "Processing schema record with serial types: {:?}",
                serial_types
            );

            // Schema table has 5 columns: type, name, tbl_name, rootpage, sql
            // We need columns 2 (name) and 4 (sql)
            if let Some(type_str) = record.read_string_field(serial_types[0])? {
                info!("Record type: {}", type_str);
                if let Some(name) = record.read_string_field(serial_types[1])? {
                    info!("Table name: {}", name);
                    if let Some(tbl_name) = record.read_string_field(serial_types[2])? {
                        info!("Table tbl_name: {}", tbl_name);
                        if name == table_name {
                            info!("Found matching table '{}', reading SQL", table_name);
                            if let Some(sql) = record.read_string_field(serial_types[4])? {
                                info!("Found SQL for table: {}", sql);
                                return TableSchema::parse(name, sql);
                            }
                        }
                    }
                }
            }
        }

        Err(anyhow!("Table not found: {}", table_name))
    }

    fn read_cell(&self, page: &[u8], cell_index: usize, header_offset: usize) -> Result<Vec<u8>> {
        // Read B-tree page header
        let btree_header = BTreePageHeader::parse(&page[header_offset..])?;

        // Get cell pointers and sort them
        let mut cell_pointers = self.read_cell_pointers(page, btree_header, header_offset);
        cell_pointers.sort_unstable();

        // Get start of current cell
        let cell_start = cell_pointers[cell_index];

        // Read the payload size varint
        let mut record = Record::new(&page[cell_start..]);
        let total_payload_size = record.read_varint()? as usize;
        let header_size = record.position();

        info!(
            "Cell at index {} starts at {} with total payload size {}",
            cell_index, cell_start, total_payload_size
        );

        // Calculate local payload size
        let max_local = (self.page_size - 35) * 64 / 255 - 23;
        let min_local = ((self.page_size - 12) * 32 / 255) - 23;

        let local_payload_size = if total_payload_size <= max_local {
            total_payload_size
        } else {
            min_local + ((total_payload_size - min_local) % (self.page_size - 4))
        };

        info!("Local payload size: {}", local_payload_size);

        // Read the local portion of the cell
        let cell_data = page[cell_start..cell_start + local_payload_size + header_size].to_vec();

        Ok(cell_data)
    }
}
