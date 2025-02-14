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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::{Block, BlockIterator};
use crate::key::{KeyBytes, KeySlice, TS_RANGE_BEGIN};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

pub(crate) const SIZEOF_U64: usize = size_of::<u64>();
pub(crate) const SIZEOF_U32: usize = size_of::<u32>();
pub(crate) const SIZEOF_U16: usize = size_of::<u16>();

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for meta in block_meta {
            let data = meta.encode();
            buf.put(data.as_slice())
        }
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.put_u64(self.offset as u64);

        buf.put_u16(self.first_key.key_len() as u16);
        buf.put(self.first_key.key_ref());
        buf.put_u64(self.first_key.ts());

        buf.put_u16(self.last_key.key_len() as u16);
        buf.put(self.last_key.key_ref());
        buf.put_u64(self.last_key.ts());
        buf
    }

    // 参考代码
    // fn decode(buf: &[u8]) -> Self {
    //     let offset = (&buf[0..SIZEOF_U64]).get_u64() as usize;
    //     let first_key_len = (&buf[SIZEOF_U64..SIZEOF_U64 + SIZEOF_U16]).get_u16() as usize;
    //     let first_key_begin = SIZEOF_U64 + SIZEOF_U16;
    //     let first_key =
    //         KeyVec::from_vec(buf[first_key_begin..first_key_begin + first_key_len].to_vec())
    //             .into_key_bytes();
    //
    //     let last_key_len_begin = first_key_begin + first_key_len;
    //     let last_key_len = (&buf[last_key_len_begin..last_key_len_begin + SIZEOF_U16])
    //         .get_u16() as usize;
    //     let last_key_begin = last_key_len_begin + SIZEOF_U16;
    //     let last_key_end = last_key_begin + last_key_len;
    //     let last_key = KeyVec::from_vec(buf[last_key_begin..last_key_end].to_vec()).into_key_bytes();
    //     Self {
    //         offset,
    //         first_key,
    //         last_key,
    //     }
    // }
    fn decode(mut buf: impl Buf) -> Self {
        let offset = buf.get_u64() as usize;
        let first_key_len = buf.get_u16() as usize;
        let first_key = buf.copy_to_bytes(first_key_len);
        let first_key_ts = buf.get_u64();
        let first_key = KeyBytes::from_bytes_with_ts(first_key, first_key_ts);
        let last_key_len = buf.get_u16() as usize;
        let last_key = buf.copy_to_bytes(last_key_len);
        let last_key_ts = buf.get_u64();
        let last_key = KeyBytes::from_bytes_with_ts(last_key, last_key_ts);
        Self {
            offset,
            first_key,
            last_key,
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut metas: Vec<BlockMeta> = Vec::new();
        let mut buf = buf;
        while buf.has_remaining() {
            let meta = BlockMeta::decode(&mut buf);
            metas.push(meta);
        }
        metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let file_size = file.size();
        let u32_sizeof = SIZEOF_U32 as u64;

        let meta_data_offset = file.read(file_size - u32_sizeof, u32_sizeof)?;
        let meta_data_offset = meta_data_offset.as_slice().get_u32() as u64;
        let offset = meta_data_offset;
        let _id = file.read(offset, u32_sizeof)?;
        let _id = _id.as_slice().get_u32() as usize;
        if _id != id {
            bail!("id mismatch");
        }

        let offset = offset + u32_sizeof;

        let meta_len = file.read(offset, u32_sizeof)?;
        let meta_len = meta_len.as_slice().get_u32() as usize as u64;
        let meta_data = file.read(offset + u32_sizeof, meta_len)?;
        let meta_checksum = file.read(offset + u32_sizeof + meta_len, u32_sizeof)?;

        let meta_checksum = meta_checksum.as_slice().get_u32();
        if meta_checksum != crc32fast::hash(&meta_data) {
            bail!("meta checksum mismatch");
        }

        let offset = offset + u32_sizeof + meta_len + u32_sizeof;
        let bloom_len = file.read(offset, u32_sizeof)?;
        let bloom_len = bloom_len.as_slice().get_u32() as usize as u64;
        let bloom_data = file.read(offset + u32_sizeof, bloom_len)?;
        let bloom_checksum = file.read(offset + u32_sizeof + bloom_len, u32_sizeof)?;
        let bloom_checksum = bloom_checksum.as_slice().get_u32();
        if bloom_checksum != crc32fast::hash(&bloom_data) {
            bail!("bloom checksum mismatch");
        }

        let block_meta = BlockMeta::decode_block_meta(&*meta_data);
        let bloom = Bloom::decode(&bloom_data)?;

        let first_key = block_meta
            .first()
            .map(|x| x.first_key.clone())
            .unwrap_or_default();
        let last_key = block_meta
            .last()
            .map(|x| x.last_key.clone())
            .unwrap_or_default();

        Ok(Self {
            file,
            block_meta,
            block_meta_offset: meta_data_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.block_meta.len() {
            return Err(anyhow::anyhow!("block_idx out of range"));
        }
        let begin = self.block_meta[block_idx].offset;
        let end = if block_idx + 1 < self.block_meta.len() {
            self.block_meta[block_idx + 1].offset
        } else {
            self.block_meta_offset
        } - SIZEOF_U32;

        let meta = &self.block_meta[block_idx];
        let data = self.file.read(begin as u64, (end - begin) as u64)?;
        let checksum = self.file.read(end as u64, SIZEOF_U32 as u64)?;
        let checksum = checksum.as_slice().get_u32();
        if checksum != crc32fast::hash(&data) {
            bail!("block checksum mismatch");
        }
        Ok(Arc::new(Block::decode(&data)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.block_meta.len() {
            return Err(anyhow::anyhow!("block_idx out of range"));
        }
        if let Some(cache) = &self.block_cache {
            let cache_key = (self.sst_id(), block_idx);
            let block = cache.get(&cache_key);
            let block = if let Some(block) = block {
                block
            } else {
                // 总感觉怪怪的, 为什么 cache.get_with 是 FnOnce 的.
                let b = self.read_block(block_idx)?;
                cache.insert(cache_key, b);
                cache.get(&cache_key).unwrap()
            };
            Ok(block)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let mut l = 0;
        let mut h = self.block_meta.len();
        while l < h {
            let mid = l + (h - l) / 2;
            let mid_key = self.block_meta[mid].last_key.as_key_slice();
            if mid_key < key {
                l = mid + 1;
            } else {
                h = mid;
            }
        }
        l
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    pub fn get(&self, _key: &[u8]) -> Option<Bytes> {
        if self.first_key().key_ref() > _key || self.last_key().key_ref() < _key {
            return None;
        }
        let key = KeySlice::from_slice(_key, 0); // TODO 处理时间

        if let Some(bloom) = &self.bloom {
            let hash = farmhash::fingerprint32(key.key_ref());
            if !bloom.may_contain(hash) {
                return None;
            }
        }

        let idx = self.find_block_idx(key);
        let block = self.read_block_cached(idx).ok();
        if let Some(block) = block {
            let iter = BlockIterator::create_and_seek_to_key(block, key);
            if iter.is_valid() && iter.key() == key {
                Some(Bytes::from(iter.value().to_vec()))
            } else {
                None
            }
        } else {
            None
        }
    }
}
