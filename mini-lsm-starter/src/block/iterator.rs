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

impl Block {
    fn get_key(&self, idx: usize) -> KeyVec {
        let offset = self.offsets[idx] as usize;
        let entry = self.data[offset..].as_ref();

        let key_len_begin = 0;
        let key_len_end = key_len_begin + SIZEOF_U16;
        let key_len = entry[key_len_begin..key_len_end].as_ref().get_u16() as usize;

        let key_begin = key_len_end;
        let key_end = key_begin + key_len;

        let key = &entry[key_begin..key_end];
        KeyVec::from_vec(key.to_vec())
    }
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            first_key: block.get_key(0),
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
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
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let mut l = 0;
        let mut h = self.block.offsets.len();
        let key = key.to_key_vec();
        while l < h {
            let mid = l + (h - l) / 2;
            let mid_key = self.block.get_key(mid);
            if mid_key < key {
                l = mid + 1;
            } else {
                h = mid;
            }
        }
        self.seek_to(l);
    }

    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key = KeyVec::new();
            self.value_range = (0, 0);
            return;
        }
        let offset = self.block.offsets[idx] as usize;

        let entry = self.block.data[offset..].as_ref();

        // 相对偏移量
        let key_len_begin = 0;
        let key_len_end = key_len_begin + SIZEOF_U16;
        let key_len = entry[key_len_begin..key_len_end].as_ref().get_u16() as usize;

        let key_begin = key_len_end;
        let key_end = key_begin + key_len;

        let key = entry[key_begin..key_end].as_ref();

        let value_len_begin = key_end;
        let value_len_end = value_len_begin + SIZEOF_U16;
        let value_len = entry[value_len_begin..value_len_end].as_ref().get_u16() as usize;

        // 转换成绝对偏移量
        let value_begin = offset + value_len_end;
        self.value_range = (value_begin, value_begin + value_len);
        self.key = KeyVec::from_vec(key.to_vec());
        self.idx = idx;
    }
}
