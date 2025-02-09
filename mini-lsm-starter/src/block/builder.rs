// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;

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
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        let is_first = self.first_key.is_empty();
        if is_first {
            self.first_key = key.to_key_vec();
            self.offsets.push(0);
        }

        let mut key = key;
        let mut new_key = key.raw_ref().to_vec();
        if !is_first {
            let overlap_len = Self::compute_overlap(self.first_key.as_key_slice(), key);
            let rest_len = key.len() - overlap_len;
            let mut k = key.raw_ref();
            k = &k[overlap_len..];
            new_key = (overlap_len as u16).to_be_bytes().to_vec();
            new_key.put_u16(rest_len as u16);
            new_key.put(k);
        }

        key = KeySlice::from_slice(new_key.as_slice());
        let key_len = key.len();
        let value_len = value.len();

        let old_offset = self.offsets.last().unwrap();
        let new_offset = (*old_offset as usize) + key_len + value_len + 4;
        let total_size = new_offset + self.offsets.len() * 2;
        if !is_first && total_size >= self.block_size {
            return false;
        }
        self.data.put_u16(key_len as u16);
        self.data.put(key.raw_ref());
        self.data.put_u16(value_len as u16);
        self.data.put(value);
        self.offsets.push(new_offset as u16);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    fn compute_overlap(a: KeySlice, b: KeySlice) -> usize {
        a.raw_ref()
            .iter()
            .zip(b.raw_ref())
            .take_while(|(a, b)| a == b)
            .count()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets[..self.offsets.len() - 1].to_vec(),
        }
    }
}
