#![allow(dead_code)]
// REMOVE THIS LINE after fully implementing this functionality
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

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use nom::AsBytes;
use parking_lot::Mutex;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(_path)
            .context("Failed to create wal file")?;
        let file = BufWriter::new(file);
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(_path)
            .context("Failed to create wal file")?;

        let mut contents = Vec::new();
        file.read_to_end(&mut contents)?;
        let mut read = &*contents;
        while read.has_remaining() {
            let (key, value) = Self::decode(&mut read)?;
            _skiplist.insert(key, value);
        }

        let file = BufWriter::new(file);
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let key_len = _key.len() as u32;
        let value_len = _value.len() as u32;
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(_key);
        hasher.update(_value);
        let checksum = hasher.finalize();
        let mut record: Vec<u8> = Vec::with_capacity(12 + key_len as usize + value_len as usize);
        record.put_u32(key_len);
        record.put(_key);
        record.put_u32(value_len);
        record.put(_value);
        record.put_u32(checksum);
        let mut file = self.file.lock();
        file.write_all(&record)?;
        Ok(())
    }

    fn decode(mut record: impl Buf) -> Result<(Bytes, Bytes)> {
        let key_len = record.get_u32() as usize;
        let key = record.copy_to_bytes(key_len);
        let value_len = record.get_u32() as usize;
        let value = record.copy_to_bytes(value_len);
        let checksum = record.get_u32();

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(key.as_bytes());
        hasher.update(value.as_bytes());
        let crc32 = hasher.finalize();

        if crc32 != checksum {
            bail!("checksum mismatch");
        }
        Ok((key, value))
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        for (key, value) in _data {
            self.put(key, value)?;
        }
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_data()?;
        Ok(())
    }
}
