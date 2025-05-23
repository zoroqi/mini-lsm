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

use std::collections::HashMap;
use std::fs::{File, create_dir};
use std::ops::Bound;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{KeySlice, TS_DEFAULT, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{MemTable, vec_map_bound};
use crate::mvcc::LsmMvccInner;
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};
use anyhow::{Context, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(())?;
        let mut lock = self.flush_thread.lock();
        if let Some(handle) = lock.take() {
            handle.join().map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
        self.compaction_notifier.send(())?;
        let mut lock = self.compaction_thread.lock();
        if let Some(handle) = lock.take() {
            handle.join().map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
        if self.inner.options.enable_wal {
            self.inner.sync()?;
            self.inner.sync_dir()?;
            return Ok(());
        }

        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        while {
            let state = self.inner.state.read();
            !state.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            match create_dir(path) {
                Ok(_) => {}
                Err(e) => {
                    return Err(anyhow::anyhow!("failed to create DB directory: {}", e));
                }
            }
        }
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let (manifest, records) = Manifest::recover(path.join("MANIFEST"))?;

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(TS_DEFAULT)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        storage.load_manifest(records)?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(self: &Arc<LsmStorageInner>, _key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.new_txn()?;
        txn.get(_key)
    }
    pub fn get_with_ts(&self, _key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        let _key = KeySlice::from_slice(_key, read_ts);
        let state = self.state.read();
        let mut value = state.memtable.get(_key);
        if value.is_none() {
            value = state
                .imm_memtables
                .iter()
                .map(|memtable| memtable.get(_key))
                .find_map(|v| v);
        }

        let sst_get = |sst_ids: Vec<usize>| -> Option<Bytes> {
            sst_ids
                .iter()
                .filter_map(|id| state.sstables.get(id))
                .find_map(|sst| sst.get(_key))
        };

        if value.is_none() {
            value = sst_get(state.l0_sstables.clone());
        }

        if value.is_none() {
            value = state
                .levels
                .iter()
                .find_map(|(_, level)| sst_get(level.clone()));
        }

        value
            .map(|v| if v.is_empty() { None } else { Some(v) })
            .map(Ok)
            .unwrap_or(Ok(None))
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.write_batch_inner(batch).map(|_| ())
    }

    pub fn write_batch_inner<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<u64> {
        let _lck = self.mvcc().write_lock.lock();
        let ts = self.mvcc().latest_commit_ts() + 1;
        let empty = Bytes::new();
        let batch = batch
            .iter()
            .map(|record| match record {
                WriteBatchRecord::Put(key, value) => {
                    (KeySlice::from_slice(key.as_ref(), ts), value.as_ref())
                }
                WriteBatchRecord::Del(key) => {
                    (KeySlice::from_slice(key.as_ref(), ts), empty.as_ref())
                }
            })
            .collect::<Vec<(KeySlice, &[u8])>>();

        let new_size = {
            let state = self.state.write();
            state.memtable.put_batch(batch.as_ref())?;
            state.memtable.approximate_size()
        };

        if new_size >= self.options.target_sst_size {
            let lock = self.state_lock.lock();
            self.force_freeze_memtable(&lock)?;
        }
        self.mvcc().update_commit_ts(ts);
        Ok(ts)
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        self.write_batch_inner(&[WriteBatchRecord::Put(_key, _value)])
            .map(|_| ())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.write_batch_inner(&[WriteBatchRecord::Del(_key)])
            .map(|_| ())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_id = self.next_sst_id();
        let new_memtable = if self.options.enable_wal {
            Arc::new(MemTable::create_with_wal(new_id, self.path_of_wal(new_id))?)
        } else {
            Arc::new(MemTable::create(new_id))
        };

        let mut state = self.state.write();
        let state_mut = Arc::make_mut(&mut state);
        // let mut state_mut = state.as_ref().clone();
        let old_memtable = std::mem::replace(&mut state_mut.memtable, new_memtable);
        // let old_memtable = Arc::clone(&state_mut.memtable);
        // state_mut.memtable = new_memtable;
        state_mut.imm_memtables.insert(0, old_memtable);
        // *state = Arc::new(state_mut);
        if let Some(m) = &self.manifest {
            m.add_record(_state_lock_observer, ManifestRecord::NewMemtable(new_id))?;
            self.sync_dir()?;
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _lock = self.state_lock.lock();
        let memtable = {
            let guard = self.state.read();
            let memtable = guard.imm_memtables.last();
            if memtable.is_none() {
                return Ok(());
            }

            memtable.unwrap().clone()
        };
        let mut builder = SsTableBuilder::new(self.options.block_size);
        memtable.flush(&mut builder)?;
        let sst_id = memtable.id();
        let sst = builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;

        {
            let mut state = self.state.write();
            let mut guard = state.as_ref().clone();
            let mem = guard.imm_memtables.pop().unwrap();
            assert_eq!(mem.id(), sst_id);
            guard.sstables.insert(sst_id, Arc::new(sst));
            if self.compaction_controller.flush_to_l0() {
                guard.l0_sstables.insert(0, sst_id);
            } else {
                guard.levels.insert(0, (sst_id, vec![sst_id]));
            }

            if let Some(m) = &self.manifest {
                m.add_record(&_lock, ManifestRecord::Flush(sst_id))?;
            }

            *state = Arc::new(guard);
        }
        self.sync_dir()?;

        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self.mvcc().new_txn(self.clone(), self.options.serializable))
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        self: &Arc<LsmStorageInner>,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<TxnIterator> {
        let txn = self.new_txn()?;
        txn.scan(_lower, _upper)
    }

    pub fn scan_with_ts(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let lower = vec_map_bound(_lower, TS_RANGE_BEGIN);
        let upper = vec_map_bound(_upper, TS_RANGE_END);

        let mut mem_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);

        let lower = match lower {
            Excluded(key) => Excluded(KeySlice::from_slice(key.key_ref(), TS_RANGE_END)),
            _ => lower,
        };

        let upper = match upper {
            Excluded(key) => Excluded(KeySlice::from_slice(key.key_ref(), TS_RANGE_BEGIN)),
            _ => upper,
        };
        let iter = snapshot.memtable.scan(lower, upper);
        mem_iters.push(iter);

        snapshot
            .imm_memtables
            .iter()
            .map(|m| m.scan(lower, upper))
            .for_each(|i| mem_iters.push(i));

        let mem_iter = MergeIterator::create(mem_iters.into_iter().map(Box::new).collect());

        let level0_sst = snapshot
            .l0_sstables
            .iter()
            .map(|id| snapshot.sstables[id].clone())
            .filter(|sst| Self::range_overlap(_lower, _upper, sst.clone()))
            .map(|sst| match _lower {
                Included(key) => SsTableIterator::create_and_seek_to_key(
                    sst,
                    KeySlice::from_slice(key, TS_RANGE_BEGIN),
                ),
                Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        sst,
                        KeySlice::from_slice(key, TS_RANGE_END),
                    );
                    if iter.is_ok() {
                        let i = iter.as_mut().unwrap();
                        if i.is_valid() && i.key().key_ref() <= key {
                            let _ = i.next();
                        }
                    }
                    iter
                }
                _ => SsTableIterator::create_and_seek_to_first(sst),
            })
            .collect::<Result<Vec<SsTableIterator>>>();

        if level0_sst.is_err() {
            return Err(level0_sst.err().unwrap());
        }

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level) in &snapshot.levels {
            let sst = level
                .iter()
                .map(|id| snapshot.sstables[id].clone())
                .filter(|sst| Self::range_overlap(_lower, _upper, sst.clone()))
                .collect::<Vec<Arc<SsTable>>>();
            let iter = SstConcatIterator::create_and_seek_to_first(sst)?;
            level_iters.push(Box::new(iter));
        }

        let level0_iter = MergeIterator::create(level0_sst?.into_iter().map(Box::new).collect());
        let mem_level0 = TwoMergeIterator::create(mem_iter, level0_iter)?;

        let level_concat_iter = MergeIterator::create(level_iters);
        let inner = TwoMergeIterator::create(mem_level0, level_concat_iter)?;

        LsmIterator::new(inner, _upper, read_ts).map(FusedIterator::new)
    }

    fn range_overlap(_lower: Bound<&[u8]>, _upper: Bound<&[u8]>, sst: Arc<SsTable>) -> bool {
        let first_key = sst.first_key().key_ref();
        let last_key = sst.last_key().key_ref();
        match (_lower, _upper) {
            (Included(lower), Included(upper)) => first_key <= upper && last_key >= lower,
            (Included(lower), Excluded(upper)) => first_key < upper && last_key >= lower,
            (Included(lower), Unbounded) => last_key >= lower,

            (Excluded(lower), Included(upper)) => first_key <= upper && last_key > lower,
            (Excluded(lower), Excluded(upper)) => first_key < upper && last_key > lower,
            (Excluded(lower), Unbounded) => last_key > lower,

            (Unbounded, Included(upper)) => first_key <= upper,
            (Unbounded, Excluded(upper)) => first_key < upper,
            (Unbounded, Unbounded) => true,
        }
    }

    fn load_manifest(&self, records: Vec<ManifestRecord>) -> Result<()> {
        let mut state = self.state.write().as_ref().clone();
        let mut all_memtable_id = Vec::new();
        let mut all_flush_id = std::collections::HashSet::new();
        for record in records {
            match record {
                ManifestRecord::Flush(id) => {
                    if self.compaction_controller.flush_to_l0() {
                        state.l0_sstables.insert(0, id);
                    } else {
                        state.levels.insert(0, (id, vec![id]));
                    }
                    all_flush_id.insert(id);
                }
                ManifestRecord::Compaction(task, ids) => {
                    let (_state, _) = self.compaction_controller.apply_compaction_result(
                        &state,
                        &task,
                        ids.as_slice(),
                        true,
                    );
                    state = _state
                }
                ManifestRecord::NewMemtable(id) => {
                    all_memtable_id.push(id);
                }
            }
        }
        let mut max_id = 0;
        let mut max_ts = TS_DEFAULT;
        // load sst
        let mut load = |id: usize| -> Result<()> {
            let path = self.path_of_sst(id);
            let file =
                FileObject::open(&path).with_context(|| format!("Failed to open sst: {:?}", id))?;
            let sst = SsTable::open(id, Some(self.block_cache.clone()), file)?;
            max_ts = max_ts.max(sst.max_ts());
            state.sstables.insert(id, Arc::new(sst));
            max_id = max_id.max(id);
            Ok(())
        };

        for &id in state.l0_sstables.iter() {
            load(id)?;
        }
        for (_, level) in &state.levels {
            for &id in level.iter() {
                load(id)?;
            }
        }

        self.next_sst_id
            .store(max_id + 1, std::sync::atomic::Ordering::SeqCst);

        if self.options.enable_wal {
            let all_memtable_id = all_memtable_id
                .iter()
                .filter(|id| !all_flush_id.contains(id))
                .copied()
                .rev()
                .collect::<Vec<_>>();
            if !all_memtable_id.is_empty() {
                let mut imm_memtable = Vec::with_capacity(all_memtable_id.len());
                for id in all_memtable_id {
                    let mem = MemTable::recover_from_wal(id, self.path_of_wal(id))?;
                    max_ts = max_ts.max(mem.max_ts());
                    imm_memtable.push(Arc::new(mem));
                }
                let (m, imm) = imm_memtable.split_at(1);
                state.memtable = m[0].clone();
                state.imm_memtables = imm.to_vec();
            }
        }

        if let CompactionController::Leveled(_) = self.compaction_controller {
            for (_, level) in &mut state.levels {
                level.sort_by(|x, y| {
                    state
                        .sstables
                        .get(x)
                        .unwrap()
                        .first_key()
                        .cmp(state.sstables.get(y).unwrap().first_key())
                });
            }
        }

        *self.state.write() = Arc::new(state);
        if let Some(mvcc) = &self.mvcc {
            mvcc.update_commit_ts(max_ts);
        }
        Ok(())
    }
}
