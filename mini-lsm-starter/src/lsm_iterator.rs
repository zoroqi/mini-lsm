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
/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    // _lower: Bound<Bytes>,
    _upper: Bound<Bytes>,
    end: bool,
    read_ts: u64,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, up: Bound<&[u8]>, read_ts: u64) -> Result<Self> {
        let mut n = Self {
            inner: iter,
            _upper: map_bound(up),
            end: false,
            read_ts,
        };
        n.move_del_key(Vec::new())?;
        n.check_end();
        Ok(n)
    }

    fn move_del_key(&mut self, prev_key: Vec<u8>) -> Result<()> {
        let mut prev_key = prev_key;
        while self.is_valid()
            && (self.value().is_empty()
                || self.inner.key().ts() > self.read_ts
                || (self.inner.key().key_ref() == prev_key))
        {
            // 当 value 为空时, 并且 key 的时间戳小于等于 read_ts 时, 需要调整 prev_key.
            // 表示这个 key 已经被删除, 之后再出现相同的 key 直接跳过就可以了.
            if self.inner.value().is_empty() && self.inner.key().ts() <= self.read_ts {
                prev_key = self.inner.key().key_ref().to_vec();
            }
            self.inner.next()?;
        }
        Ok(())
    }

    fn check_end(&mut self) {
        if !self.is_valid() {
            self.end = true;
            return;
        }
        let key = self.inner.key().key_ref();

        let end = match &self._upper {
            Bound::Included(h) => key > h,
            Bound::Excluded(h) => key >= h,
            Bound::Unbounded => false,
        };
        self.end = end;
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
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        let prev_key = self.key().to_vec();
        self.inner.next()?;
        self.move_del_key(prev_key)?;
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
