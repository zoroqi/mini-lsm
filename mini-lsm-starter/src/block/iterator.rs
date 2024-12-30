#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use super::Block;
use crate::key::{KeySlice, KeyVec};
use bytes::Buf;
use std::sync::Arc;

pub(crate) const SIZEOF_U32: usize = std::mem::size_of::<u32>();
pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut block = Self::new(block);
        block.seek_to_first();
        block
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut block = Self::new(block);
        block.seek_to_key(key);
        block
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        self.block.data[self.value_range.0..self.value_range.1].as_ref()
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.next();
        self.first_key = self.key.clone();
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx == self.block.offsets.len() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            return;
        }

        let next_entry_offset_begin = self.block.offsets[self.idx] as usize;

        let next_entry = self.block.data[next_entry_offset_begin..].as_ref();

        let nkey_len_begin = 0;
        let nkey_len_end = nkey_len_begin + SIZEOF_U16;
        let nkey_len = next_entry[nkey_len_begin..nkey_len_end].as_ref().get_u16() as usize;
        let nkey_begin = nkey_len_end;
        let nkey_end = nkey_begin + nkey_len;
        let key = next_entry[nkey_begin..nkey_end].as_ref();

        let nvalue_len_begin = nkey_end;
        let nvalue_len_end = nvalue_len_begin + SIZEOF_U16;
        let value_len = next_entry[nvalue_len_begin..nvalue_len_end]
            .as_ref()
            .get_u16() as usize;

        // 转换成绝对偏移量
        let value_begin = next_entry_offset_begin + nvalue_len_end;
        self.value_range = (value_begin, value_begin + value_len);

        self.key = KeyVec::from_vec(key.to_vec());
        self.idx = self.idx + 1;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.idx = 0;
        self.next();
        while self.idx < self.block.offsets.len() && self.key() < key {
            self.next();
        }
    }
}
