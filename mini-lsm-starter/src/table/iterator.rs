#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    fn new(table: Arc<SsTable>) -> Self {
        let first_block = table.read_block_cached(0);
        Self {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(first_block.unwrap()),
            blk_idx: 0,
        }
    }
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let mut iter = Self::new(table);
        iter.seek_to_first()?;
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.seek_to(0)
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let mut iter = Self::new(table);
        iter.seek_to_key(key)?;
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let idx = self.table.find_block_idx(key);
        self.seek_to(idx)?;
        self.blk_iter.seek_to_key(key);
        Ok(())
    }

    fn seek_to(&mut self, idx: usize) -> Result<()> {
        if idx >= self.table.num_of_blocks() {
            self.blk_idx = idx;
            return Ok(());
        }
        let block = self.table.read_block_cached(idx)?;
        let iter = BlockIterator::create_and_seek_to_first(block);
        self.blk_iter = iter;
        self.blk_idx = idx;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        assert!(
            self.blk_idx < self.table.num_of_blocks(),
            "invalid iterator"
        );
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        assert!(
            self.blk_idx < self.table.num_of_blocks(),
            "invalid iterator"
        );
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_idx < self.table.num_of_blocks() && self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.seek_to(self.blk_idx + 1)?;
        }
        Ok(())
    }
}

impl SsTableIterator {
    pub fn scan(&mut self, key: KeySlice) -> Result<()> {
        self.seek_to_key(key)?;
        Ok(())
    }
}
