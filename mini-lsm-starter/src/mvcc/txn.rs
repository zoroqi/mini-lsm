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

use crate::lsm_storage::WriteBatchRecord::{Del, Put};
use crate::mvcc::CommittedTxnData;
use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::LsmStorageInner,
};
use anyhow::{Result, bail};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;
use std::option::Option;
use std::sync::atomic::Ordering;
use std::{
    collections::HashSet,
    ops::Bound,
    sync::{Arc, atomic::AtomicBool},
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn new(read_ts: u64, inner: Arc<LsmStorageInner>, serializable: bool) -> Self {
        Transaction {
            read_ts,
            inner,
            local_storage: Arc::new(SkipMap::new()),
            committed: Arc::new(AtomicBool::new(false)),
            key_hashes: if serializable {
                Some(Mutex::new((HashSet::new(), HashSet::new())))
            } else {
                None
            },
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("Transaction already committed");
        }
        self.add_read_hash(key);

        if let Some(entry) = self.local_storage.get(key) {
            let v = entry.value();
            if v.is_empty() {
                return Ok(None);
            } else {
                return Ok(Some(v.clone()));
            }
        }

        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::SeqCst) {
            panic!("Transaction already committed");
        }

        let mut local_iter = TxnLocalIteratorBuilder {
            map: self.local_storage.clone(),
            iter_builder: |m| {
                m.range((
                    crate::mem_table::map_bound(lower),
                    crate::mem_table::map_bound(upper),
                ))
            },
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        local_iter.next()?;

        let inner_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;

        TxnIterator::create(
            self.clone(),
            TwoMergeIterator::create(local_iter, inner_iter)?,
        )
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("Transaction already committed");
        }
        self.add_write_hash(key);
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        if self.committed.load(Ordering::SeqCst) {
            panic!("Transaction already committed");
        }
        self.add_write_hash(key);
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
    }

    pub fn commit(&self) -> Result<()> {
        self.committed.store(true, Ordering::SeqCst);
        let _commit_lock = self.inner.mvcc().commit_lock.lock();
        let serializable = if let Some(hashes) = &self.key_hashes {
            let lock = hashes.lock();
            let write_hash = lock.1.clone();
            if !write_hash.is_empty() {
                let read_hash = lock.0.clone();
                let txns = self.inner.mvcc().committed_txns.lock();
                if txns.iter().any(|(_, v)| {
                    if self.read_ts >= v.commit_ts {
                        return false;
                    }
                    let key_hashes = &v.key_hashes;
                    key_hashes.iter().any(|h| read_hash.contains(h))
                }) {
                    bail!("serializable check faild");
                }
            }
            true
        } else {
            false
        };

        let iter = self.local_storage.iter();
        let batch = iter
            .into_iter()
            .map(|e| {
                if e.value().is_empty() {
                    Del(e.key().to_vec())
                } else {
                    Put(e.key().to_vec(), e.value().to_vec())
                }
            })
            .collect::<Vec<_>>();

        let commit_ts = self.inner.write_batch_inner(&batch)?;
        if serializable {
            let lock = &self.key_hashes.as_ref().unwrap().lock();
            let write_hash = lock.1.clone();
            let mut txns = self.inner.mvcc().committed_txns.lock();
            txns.insert(
                commit_ts,
                CommittedTxnData {
                    key_hashes: write_hash,
                    read_ts: self.read_ts,
                    commit_ts,
                },
            );
            let watermark = self.inner.mvcc().watermark();
            while let Some(entry) = txns.first_entry() {
                if *entry.key() < watermark {
                    entry.remove();
                } else {
                    break;
                }
            }
        }
        Ok(())
    }

    fn add_read_hash(&self, key: &[u8]) {
        if let Some(key_hashes) = &self.key_hashes {
            let hash = farmhash::hash32(key);
            let mut key_hashes = key_hashes.lock();
            key_hashes.0.insert(hash);
        }
    }

    fn add_write_hash(&self, key: &[u8]) {
        if let Some(key_hashes) = &self.key_hashes {
            let hash = farmhash::hash32(key);
            let mut key_hashes = key_hashes.lock();
            key_hashes.1.insert(hash);
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0[..]
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| match iter.next() {
            Some(en) => (en.key().clone(), en.value().clone()),
            None => (Bytes::new(), Bytes::new()),
        });
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

pub struct TxnIterator {
    _txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        if iter.is_valid() {
            txn.add_read_hash(iter.key());
        }
        Ok(TxnIterator { _txn: txn, iter })
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        while self.is_valid() && self.value().is_empty() {
            self.iter.next()?;
        }
        if self.is_valid() {
            self._txn.add_read_hash(self.key());
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
