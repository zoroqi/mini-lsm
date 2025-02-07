#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

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
                    .write(true)
                    .read(true)
                    .append(true)
                    .open(_path)
                    .context("Failed to create manifest file")?,
            )),
        })
    }

    pub fn recover(_path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .write(true)
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
