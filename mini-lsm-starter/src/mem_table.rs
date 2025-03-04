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

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

pub(crate) fn to_key_bytes(key: KeySlice) -> KeyBytes {
    key.to_key_vec().into_key_bytes()
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(crate) fn bytes_map_bound(bound: Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(x) => Bound::Included(to_key_bytes(x)),
        Bound::Excluded(x) => Bound::Excluded(to_key_bytes(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

pub(crate) fn vec_map_bound(bound: Bound<&[u8]>, ts: u64) -> Bound<KeySlice> {
    match bound {
        Bound::Included(x) => Bound::Included(KeySlice::from_slice(x, ts)),
        Bound::Excluded(x) => Bound::Excluded(KeySlice::from_slice(x, ts)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(_id: usize) -> Self {
        MemTable {
            map: Arc::new(SkipMap::new()),
            wal: None,
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        let wal = Wal::create(_path)?;
        Ok(MemTable {
            map: Arc::new(SkipMap::new()),
            wal: Some(wal),
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(_id: usize, _path: impl AsRef<Path>) -> Result<Self> {
        let map: SkipMap<KeyBytes, Bytes> = SkipMap::new();
        let wal = Wal::recover(_path, &map)?;
        Ok(MemTable {
            map: Arc::new(map),
            wal: Some(wal),
            id: _id,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        self.scan(
            vec_map_bound(lower, TS_DEFAULT),
            vec_map_bound(upper, TS_DEFAULT),
        )
    }

    /// Get a value by key.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        self.map
            .lower_bound(Bound::Included(to_key_bytes(key)).as_ref())
            .and_then(|e| {
                if e.key().key_ref() == key.key_ref() {
                    Some(e.value().clone())
                } else {
                    None
                }
            })
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, key: KeySlice, _value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, _value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut size = 0;
        for (key, value) in data.iter().copied() {
            size += value.len() + key.key_ref().len();
            self.map
                .insert(to_key_bytes(key), Bytes::copy_from_slice(value));
        }
        self.approximate_size.fetch_add(size, Ordering::Relaxed);

        if let Some(ref wal) = self.wal {
            wal.put_batch(data)?;
        }
        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, _lower: Bound<KeySlice>, _upper: Bound<KeySlice>) -> MemTableIterator {
        let mut iter = MemTableIteratorBuilder {
            map: self.map.clone(),
            iter_builder: |m| {
                m.range((
                    bytes_map_bound(_lower).clone(),
                    bytes_map_bound(_upper).clone(),
                ))
            },
            item: (KeyBytes::new(), Bytes::new()),
        }
        .build();
        iter.next().unwrap();
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, _builder: &mut SsTableBuilder) -> Result<()> {
        self.map.iter().for_each(|entry| {
            _builder.add(entry.key().as_key_slice(), &entry.value()[..]);
        });
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size.load(Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn max_ts(&self) -> u64 {
        self.map
            .iter()
            .map(|entry| entry.key().ts())
            .max()
            .unwrap_or(TS_DEFAULT)
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> KeySlice {
        self.borrow_item().0.as_key_slice()
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| match iter.next() {
            Some(en) => (en.key().clone(), en.value().clone()),
            None => (KeyBytes::new(), Bytes::new()),
        });
        self.with_mut(|x| *x.item = entry);

        Ok(())
    }
}
