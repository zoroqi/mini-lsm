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

use crate::key::{KeyBytes, KeySlice};
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

    pub fn recover(_path: impl AsRef<Path>, _skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
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
            let one_batch = Self::decode_batch(&mut read)?;
            for (key, value) in one_batch {
                _skiplist.insert(key, value);
            }
        }

        let file = BufWriter::new(file);
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let size = data.len();
        if size == 0 {
            return Ok(());
        }

        let mut record = Vec::new();
        record.put_u32(size as u32);

        let mut hasher = crc32fast::Hasher::new();
        for (k, v) in data.iter().copied() {
            let key = k.key_ref();
            let ts = k.ts();

            hasher.update(key);
            hasher.update(v);
            hasher.update(&ts.to_be_bytes());

            record.put_u16(key.len() as u16);
            record.put(key);
            record.put_u64(ts);
            record.put_u16(v.len() as u16);
            record.put(v);
        }
        let checksum = hasher.finalize();
        record.put_u32(checksum);
        let mut file = self.file.lock();
        file.write_all(&record)?;
        Ok(())
    }

    fn decode_batch(mut record: impl Buf) -> Result<Vec<(KeyBytes, Bytes)>> {
        let size = record.get_u32();
        let mut result: Vec<(KeyBytes, Bytes)> = Vec::with_capacity(size as usize);
        let mut hasher = crc32fast::Hasher::new();
        for _ in 0..size {
            let key_len = record.get_u16() as usize;
            let key = record.copy_to_bytes(key_len);
            let ts = record.get_u64();
            let value_len = record.get_u16() as usize;
            let value = record.copy_to_bytes(value_len);

            hasher.update(key.as_bytes());
            hasher.update(value.as_bytes());
            hasher.update(&ts.to_be_bytes());

            result.push((KeyBytes::from_bytes_with_ts(key, ts), value));
        }
        let crc32 = record.get_u32();
        if hasher.finalize() != crc32 {
            bail!("checksum mismatch");
        }
        Ok(result)
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_data()?;
        Ok(())
    }
}
