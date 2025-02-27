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

use super::Block;
use crate::block::iterator::{SIZEOF_U16, SIZEOF_U32, SIZEOF_U64};
use crate::key::{KeySlice, KeyVec};
use bytes::BufMut;

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

        let nk = if !is_first {
            // 这里遇到一个问题, 在使用 Bytes::from_static(unsafe { std::mem::transmute(key.key_ref()) }) 时, 会出现错误.
            // 错误是 key.key_ref() 返回的地址和 Vec::with_capacity 创建的 new_key 地址会有不一致的情况,
            // 导致 `unsafe precondition(s) violated: ptr::copy_nonoverlapping requires that both pointer arguments are aligned and non-null and the specified memory ranges do not overlap` 错误.
            // 不知道为什么, 不再使用 unsafe 就好了. 仅仅用一个下午学的 rust, 还不足以理解这个问题.

            // println!("1f: {:?}, k: {:?}", self.first_key.key_ref(), key.key_ref());
            let overlap_len = Self::compute_overlap(self.first_key.key_ref(), key.key_ref());
            let rest_len = key.key_len() - overlap_len;
            // let mut k = Vec::with_capacity(key.key_len());
            // for i in key.key_ref() {
            //     k.push(*i);
            // }
            let k = key.key_ref();
            // println!("1.1key_ref ptr: {:p}", k.as_ptr());
            // println!("2o: {}, r: {}", overlap_len, rest_len);
            // println!("3f: {:?}, k: {:?}", self.first_key.key_ref(), k);

            let mut new_key = Vec::with_capacity(4 + rest_len); // 预分配容量
                                                                // println!("3.1nk_ref ptr: {:p}", new_key.as_ptr());

            // println!(
            //     "4f: {:?}, k: {:?}, n:{:?}",
            //     self.first_key.key_ref(),
            //     k,
            //     new_key
            // );
            new_key.put_u16(overlap_len as u16);
            // println!("4.1nk_ref ptr: {:p}", new_key.as_ptr());
            new_key.put_u16(rest_len as u16);
            // println!("4.2nk_ref ptr: {:p}", new_key.as_ptr());
            // println!(
            //     "5f: {:?}, k: {:?}, n:{:?}",
            //     self.first_key.key_ref(),
            //     k,
            //     new_key
            // );
            new_key.put(&k[overlap_len..]);
            // println!(
            //     "6f: {:?}, k: {:?}, n:{:?}",
            //     self.first_key.key_ref(),
            //     k,
            //     new_key
            // );
            new_key
        } else {
            key.key_ref().to_vec()
        };

        let key_ts = key.ts();
        // println!("build: {:?}, key:{:?}, {}", nk, key.key_ref(),key_ts);
        // let key = KeySlice::from_slice(new_key.as_slice(), key_ts);

        let key_len = nk.len();
        let value_len = value.len();

        let old_offset = self.offsets.last().unwrap();
        let new_offset = (*old_offset as usize) + key_len + value_len + SIZEOF_U32 + SIZEOF_U64;
        let total_size = new_offset + self.offsets.len() * SIZEOF_U16;
        if !is_first && total_size >= self.block_size {
            return false;
        }
        self.data.put_u16(key_len as u16);
        self.data.put(nk.as_slice());
        self.data.put_u64(key_ts);
        self.data.put_u16(value_len as u16);
        self.data.put(value);
        self.offsets.push(new_offset as u16);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    fn compute_overlap(a: &[u8], b: &[u8]) -> usize {
        a.iter().zip(b).take_while(|(a, b)| a == b).count()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets[..self.offsets.len() - 1].to_vec(),
        }
    }
}
