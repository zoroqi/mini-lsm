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

use super::{BlockMeta, FileObject, SsTable};
use crate::key::KeyBytes;
use crate::table::bloom::Bloom;
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};
use anyhow::Result;
use bytes::BufMut;
use std::path::Path;
use std::sync::Arc;

pub(crate) const SIZEOF_U64: usize = std::mem::size_of::<u64>();
pub(crate) const SIZEOF_U32: usize = std::mem::size_of::<u32>();
pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyBytes,
    last_key: KeyBytes,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyBytes::new(),
            last_key: KeyBytes::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        assert!(!key.is_empty(), "key must not be empty");
        self.key_hashes.push(farmhash::fingerprint32(key.key_ref()));
        if self.builder.add(key, value) {
            return;
        }
        self.create_new_block();
        let _ = self.builder.add(key, value);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
            + if self.builder.is_empty() {
                0
            } else {
                self.block_size
            }
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let mut sst_build = self;
        sst_build.create_new_block();

        let data_size = sst_build.data.len() as u64;
        let mut meta_data: Vec<u8> = Vec::new();

        BlockMeta::encode_block_meta(&sst_build.meta, &mut meta_data);

        let keys = sst_build.key_hashes;
        let bloom =
            Bloom::build_from_key_hashes(&keys, Bloom::bloom_bits_per_key(keys.len(), 0.01));

        let mut bloom_data = Vec::with_capacity(bloom.filter.len() + 1);
        bloom.encode(&mut bloom_data);
        let bloom_data_size = bloom_data.len() as u64;

        let meta_size = meta_data.len() as u64;

        // | data | meta_size | meta | bloom_size | bloom | meta_extra_size | meta_extra |
        let file_size = data_size + meta_size + bloom_data_size + SIZEOF_U32 as u64 * 3;

        let mut sst =
            SsTable::create_meta_only(id, file_size, sst_build.first_key, sst_build.last_key);

        sst.block_cache = block_cache;

        sst.block_meta_offset = data_size as usize;
        sst.block_meta = sst_build.meta;
        sst.max_ts = 0;
        sst.bloom = Some(bloom);

        let mut data = sst_build.data;
        data.put_u32(id as u32);

        let meta_len = meta_data.len() as u32;
        data.put_u32(meta_len);
        data.put(&*meta_data);
        let meta_checksum = crc32fast::hash(&meta_data);
        data.put_u32(meta_checksum);

        let bloom_len = bloom_data.len() as u32;
        data.put_u32(bloom_len);
        data.put(&*bloom_data);
        let bloom_checksum = crc32fast::hash(&bloom_data);
        data.put_u32(bloom_checksum);

        let meta_extra = data_size as u32;
        data.put_u32(meta_extra);

        let file = FileObject::create(path.as_ref(), data)?;
        sst.file = file;

        Ok(sst)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }

    fn create_new_block(&mut self) {
        if self.builder.is_empty() {
            return;
        }
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let block = builder.build();
        let meta = BlockMeta {
            offset: self.data.len(),
            first_key: block.first_key().into_key_bytes(),
            last_key: block.last_key().into_key_bytes(),
        };
        // 更新 sst 的 first key 和 last key
        if self.first_key.is_empty() {
            self.first_key = block.first_key().into_key_bytes();
        }
        self.last_key = block.last_key().into_key_bytes();

        self.meta.push(meta);
        let data = block.encode();
        let date_checksum = crc32fast::hash(&data);
        self.data.put(data);
        self.data.put_u32(date_checksum);
    }
}
