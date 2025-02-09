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

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::compact::CompactionTask;
use anyhow::{Context, Result};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(_path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .append(true)
                    .open(_path)
                    .context("Failed to create manifest file")?,
            )),
        })
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(_path)
            .context("Failed to create manifest file")?;
        let mut contents = Vec::new();
        file.read_to_end(&mut contents)?;
        let json = serde_json::Deserializer::from_slice(contents.as_slice());
        let stream = json.into_iter::<ManifestRecord>();
        let mut records: Vec<ManifestRecord> = Vec::new();
        for record in stream {
            records.push(record?);
        }
        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, _record: ManifestRecord) -> Result<()> {
        let json_str = serde_json::to_vec(&_record)?;
        let mut lock = self.file.lock();
        lock.write_all(&json_str)?;
        lock.sync_data()?;
        Ok(())
    }
}
