#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::mem_table::map_bound;
use crate::table::SsTableIterator;
use crate::{
    iterators::{
        merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
};
use anyhow::bail;
use anyhow::Result;
use bytes::Bytes;
use std::ops::Bound;

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    _lower: Bound<Bytes>,
    _upper: Bound<Bytes>,
    end: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, low: Bound<&[u8]>, up: Bound<&[u8]>) -> Result<Self> {
        let mut n = Self {
            inner: iter,
            _lower: map_bound(low),
            _upper: map_bound(up),
            end: false,
        };
        n.move_del_key()?;
        // n.skip_low().unwrap();
        n.check_end();
        Ok(n)
    }

    fn move_del_key(&mut self) -> Result<()> {
        while self.is_valid() && self.value().is_empty() {
            self.inner.next()?;
        }
        Ok(())
    }

    fn check_end(&mut self) {
        if !self.is_valid() {
            self.end = true;
            return;
        }
        let key = self.key();
        let end = match &self._upper {
            Bound::Included(h) => key > h,
            Bound::Excluded(h) => key >= h,
            Bound::Unbounded => false,
        };
        self.end = end;
    }
    fn skip_low(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }
        let key = self.key();
        let remove = match &self._lower {
            Bound::Included(l) => key < l,
            Bound::Excluded(l) => key <= l,
            Bound::Unbounded => false,
        };
        if remove {
            self.inner.next()?;
            return self.skip_low();
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        if self.end {
            false
        } else {
            self.inner.is_valid()
        }
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.move_del_key()?;
        self.check_end();
        Ok(())
    }
    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        if self.has_errored {
            return false;
        }
        self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if !self.is_valid() {
            panic!("Iterator has already errored");
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if !self.is_valid() {
            panic!("Iterator has already errored");
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("Iterator has already errored");
        }
        if self.iter.is_valid() {
            if let e @ Err(_) = self.iter.next() {
                self.has_errored = true;
                return e;
            }
        }
        Ok(())
    }
    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
