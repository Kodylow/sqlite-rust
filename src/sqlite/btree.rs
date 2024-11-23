use anyhow::{anyhow, Result};
use std::io::{Read, Seek, SeekFrom};
use tracing::info;

/// Represents a B-tree page in SQLite
///
/// ## B-tree Page Structure
///
/// Each page in the database file is a B-tree page that contains:
///
/// - Page header (8-12 bytes)
/// - Cell pointer array
/// - Unallocated space
/// - Cell content area
/// - Reserved region
pub struct BTreePage {
    /// Raw page data
    data: Vec<u8>,
    /// Page type (leaf=13, interior=5)
    page_type: u8,
    /// Number of cells in page
    num_cells: u16,
}

/// Represents a B-tree page header
///
/// ## B-tree Page Header Format
///
/// - Byte 0: Page type
/// - Bytes 1-2: First freeblock offset
/// - Bytes 3-4: Number of cells
/// - Bytes 5-6: Cell content offset
/// - Byte 7: Number of fragmented free bytes
#[derive(Debug)]
pub struct BTreePageHeader {
    /// Page type (leaf=13, interior=5)
    pub page_type: u8,
    /// Offset to first freeblock
    pub first_freeblock: u16,
    /// Number of cells in page
    pub num_cells: u16,
    /// Offset to cell content area
    pub content_offset: u16,
    /// Number of fragmented free bytes
    pub fragmented_free_bytes: u8,
}

impl BTreePage {
    /// Reads a B-tree page from the given file at the specified page number
    pub fn read(file: &mut std::fs::File, page_num: u32, page_size: u16) -> Result<Self> {
        let mut page = vec![0; page_size as usize];

        // Calculate page offset
        let offset = ((page_num - 1) as u64) * (page_size as u64);
        info!("Seeking to offset: {} for page {}", offset, page_num);

        // Verify file length
        let file_len = file.seek(SeekFrom::End(0))?;
        if offset >= file_len {
            return Err(anyhow!(
                "Page offset {} exceeds file length {}",
                offset,
                file_len
            ));
        }

        // Read the page
        file.seek(SeekFrom::Start(offset))?;
        let bytes_read = file.read(&mut page)?;
        if bytes_read != page_size as usize {
            return Err(anyhow!(
                "Partial read: got {} bytes, expected {}",
                bytes_read,
                page_size
            ));
        }

        let page_type = page[0];
        let num_cells = u16::from_be_bytes([page[3], page[4]]);

        Ok(Self {
            data: page,
            page_type,
            num_cells,
        })
    }

    /// Returns the page type
    pub fn page_type(&self) -> u8 {
        self.page_type
    }

    /// Returns number of cells in the page
    pub fn num_cells(&self) -> u16 {
        self.num_cells
    }

    /// Reads and returns the cell pointer array
    pub fn read_cell_pointers(&self, header_offset: usize) -> Vec<usize> {
        let mut cell_pointers = Vec::with_capacity(self.num_cells as usize);
        let array_start = header_offset + 8;

        for i in 0..self.num_cells {
            let offset = array_start + (i as usize * 2);
            let ptr = u16::from_be_bytes([self.data[offset], self.data[offset + 1]]) as usize;
            cell_pointers.push(ptr);
        }

        cell_pointers
    }

    /// Gets child page numbers from an interior page
    pub fn get_child_pages(&self) -> Result<Vec<u32>> {
        if self.page_type != 5 {
            return Err(anyhow!("Not an interior page"));
        }

        let mut children = Vec::new();
        let array_start = 12;

        // Get child pages from cell pointers
        for i in 0..self.num_cells {
            let ptr_offset = array_start + (i as usize * 2);
            let cell_ptr =
                u16::from_be_bytes([self.data[ptr_offset], self.data[ptr_offset + 1]]) as usize;

            let child_page = u32::from_be_bytes([
                self.data[cell_ptr],
                self.data[cell_ptr + 1],
                self.data[cell_ptr + 2],
                self.data[cell_ptr + 3],
            ]);
            children.push(child_page);
        }

        // Add rightmost pointer
        let rightmost =
            u32::from_be_bytes([self.data[8], self.data[9], self.data[10], self.data[11]]);
        children.push(rightmost);

        Ok(children)
    }

    /// Gets raw page data
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

impl BTreePageHeader {
    /// Parse a B-tree page header from a byte slice
    pub fn parse(data: &[u8]) -> Result<Self> {
        if data.len() < 8 {
            return Err(anyhow!("Page header too short"));
        }

        Ok(Self {
            page_type: data[0],
            first_freeblock: u16::from_be_bytes([data[1], data[2]]),
            num_cells: u16::from_be_bytes([data[3], data[4]]),
            content_offset: u16::from_be_bytes([data[5], data[6]]),
            fragmented_free_bytes: data[7],
        })
    }

    /// Returns the offset where cell pointer array starts
    pub fn cell_pointer_array_offset(&self, header_offset: usize) -> usize {
        header_offset + 8 // 8 bytes for the header
    }
}
