#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size: block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        if key.is_empty() {
            return false;
        }
        let is_first = self.first_key.is_empty();
        if is_first {
            self.first_key = key.to_key_vec();
            self.offsets.push(0)
        }
        let key = key.raw_ref();
        let key_len = key.len();
        let value_len = value.len();

        let old_offset = self.offsets.last().unwrap();
        let new_offset = (*old_offset as usize) + key_len + value_len + 4;
        let total_size = new_offset + self.offsets.len() * 2;
        if !is_first && total_size >= self.block_size {
            return false;
        }
        self.data
            .extend_from_slice((key_len as u16).to_be_bytes().as_slice());
        self.data.extend_from_slice(key);
        self.data
            .extend_from_slice((value_len as u16).to_be_bytes().as_slice());
        self.data.extend_from_slice(value);
        self.offsets.push(new_offset as u16);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets[..self.offsets.len() - 1].to_vec(),
        }
    }
}
