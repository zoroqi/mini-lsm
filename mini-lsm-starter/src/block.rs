#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use crate::key::KeyVec;
pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;
use serde::__private::de::IdentifierDeserializer;
use std::io::Read;

pub(crate) const SIZEOF_U32: usize = std::mem::size_of::<u32>();
pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
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

        let offset = &data[len - SIZEOF_U32 - offset_len..len - SIZEOF_U32]
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect::<Vec<u16>>();

        let entry = &data[..len - SIZEOF_U32 - offset_len];
        Self {
            data: entry.to_vec(),
            offsets: offset.to_vec(),
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

        if idx == 0 {
            let key = &entry[key_begin..key_end];
            return KeyVec::from_vec(key.to_vec());
        }

        let mut key = entry[key_begin..key_end].as_ref();
        let overlap_len = key.get_u16() as usize;
        let rest_len = key.get_u16() as usize;
        let key = &key[..rest_len];
        let first = self.first_key().clone();
        let mut overlap: Vec<u8> = Vec::new();
        overlap.put(&first.raw_ref()[..overlap_len]);
        overlap.put(key);
        KeyVec::from_vec(overlap.to_vec())
    }

    pub fn first_key(&self) -> KeyVec {
        self.get_key(0)
    }
    pub fn last_key(&self) -> KeyVec {
        self.get_key(self.offsets.len() - 1)
    }
}
