use anyhow::Result;

/// Utility functions for handling SQLite variable-length integers (varints)
pub trait Varint {
    /// Read a varint from a byte slice
    fn read_varint(&self, bytes: &[u8]) -> Result<u64>;

    /// Get the size of a varint in bytes
    fn varint_size(&self, bytes: &[u8]) -> usize;
}

impl Varint for [u8] {
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

    fn varint_size(&self, bytes: &[u8]) -> usize {
        let mut size = 0;
        while size < bytes.len() && bytes[size] & 0x80 != 0 {
            size += 1;
        }
        size + 1
    }
}
