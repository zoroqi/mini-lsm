#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        if sstables.is_empty() {
            return Ok(SstConcatIterator {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        }
        let mut sst_concat = SstConcatIterator {
            current: Some(SsTableIterator::create_and_seek_to_first(
                sstables[0].clone(),
            )?),
            next_sst_idx: 1,
            sstables,
        };
        sst_concat.try_move_next()?;
        Ok(sst_concat)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut sst_concat = SstConcatIterator::create_and_seek_to_first(sstables)?;

        let index: usize = sst_concat
            .sstables
            .partition_point(|table| table.first_key().as_key_slice() <= key)
            .saturating_sub(1);

        if index >= sst_concat.sstables.len() {
            sst_concat.current = None;
            sst_concat.next_sst_idx = index;
            return Ok(sst_concat);
        }
        let iter =
            SsTableIterator::create_and_seek_to_key(sst_concat.sstables[index].clone(), key)?;
        sst_concat.current = Some(iter);
        sst_concat.next_sst_idx = index + 1;
        Ok(sst_concat)
    }

    fn try_move_next(&mut self) -> Result<()> {
        while let Some(iter) = self.current.as_mut() {
            if iter.is_valid() {
                break;
            }
            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
            } else {
                let iter = SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?;
                self.current = Some(iter);
                self.next_sst_idx += 1;
            }
        }
        Ok(())
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        assert!(self.is_valid(), "invalid iterator");
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        assert!(self.is_valid(), "invalid iterator");
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        if let Some(c) = &self.current {
            return c.is_valid();
        }
        false
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        self.try_move_next()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.sstables.len() - self.next_sst_idx + 1
    }
}
