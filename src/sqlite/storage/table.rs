use crate::sqlite::core::btree::BTreePageHeader;
use crate::sqlite::core::header::DatabaseHeader;
use crate::sqlite::core::record::Record;
use anyhow::Result;
use std::fs::File;
use std::io::prelude::*;

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
        let mut cell_pointers = Vec::with_capacity(header.num_cells as usize);
        let array_start = header.cell_pointer_array_offset(header_offset);

        for i in 0..header.num_cells {
            let offset = array_start + (i as usize * 2);
            let ptr = u16::from_be_bytes([page[offset], page[offset + 1]]) as usize;
            cell_pointers.push(ptr);
        }

        cell_pointers
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
}
