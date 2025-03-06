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

mod builder;
mod iterator;

use crate::key::KeyVec;
use crate::table::{SIZEOF_U16, SIZEOF_U32, SIZEOF_U64};
pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut result = self.data.clone();
        for &x in &self.offsets {
            result.put_u16(x);
        }
        let size = self.offsets.len() as u32;
        result.put_u32(size);
        result.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let len = data.len();
        if len == 0 {
            return Self {
                data: Vec::new(),
                offsets: Vec::new(),
            };
        }

        let entry_size = (&data[len - SIZEOF_U32..]).get_u32() as usize;
        let offset_len = entry_size * SIZEOF_U16;

        let offsets = &data[len - SIZEOF_U32 - offset_len..len - SIZEOF_U32]
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect::<Vec<u16>>();

        let entry = &data[..len - SIZEOF_U32 - offset_len];
        Self {
            data: entry.to_vec(),
            offsets: offsets.to_vec(),
        }
    }
}

impl Block {
    pub fn get_key(&self, idx: usize) -> KeyVec {
        let offset = self.offsets[idx] as usize;
        let entry = self.data[offset..].as_ref();

        let key_len_begin = 0;
        let key_len_end = key_len_begin + iterator::SIZEOF_U16;
        let key_len = entry[key_len_begin..key_len_end].as_ref().get_u16() as usize;

        let key_begin = key_len_end;
        let key_end = key_begin + key_len;

        let key = &entry[key_begin..key_end];
        let ts = entry[key_end..key_end + SIZEOF_U64].as_ref().get_u64();

        if idx == 0 {
            return KeyVec::from_vec_with_ts(key.to_vec(), ts);
        }

        let mut key = key;
        let overlap_len = key.get_u16() as usize;
        let rest_len = key.get_u16() as usize;
        let key = &key[..rest_len];
        let first = self.first_key().clone();
        let mut overlap: Vec<u8> = Vec::new();

        overlap.put(&first.key_ref()[..overlap_len]);
        overlap.put(key);

        KeyVec::from_vec_with_ts(overlap.to_vec(), ts)
    }

    pub fn first_key(&self) -> KeyVec {
        self.get_key(0)
    }
    pub fn last_key(&self) -> KeyVec {
        self.get_key(self.offsets.len() - 1)
    }
}
