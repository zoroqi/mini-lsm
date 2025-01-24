#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        // get a read lock
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };

        let build_concat = |ids: Vec<usize>| -> Result<SstConcatIterator> {
            let ssts = ids
                .iter()
                .map(|&id| snapshot.sstables[&id].clone())
                .collect();
            SstConcatIterator::create_and_seek_to_first(ssts)
        };

        let build_sst = |ids: Vec<usize>| -> Result<MergeIterator<SsTableIterator>> {
            let mut ssts: Vec<SsTableIterator> = Vec::with_capacity(ids.len());
            for &id in ids.iter() {
                let sst = snapshot.sstables[&id].clone();
                let iter = SsTableIterator::create_and_seek_to_first(sst)?;
                ssts.push(iter);
            }
            Ok(MergeIterator::create(
                ssts.into_iter().map(Box::new).collect(),
            ))
        };

        match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
                ..
            } => {
                let up_iter = build_sst(l0_sstables.clone())?;
                let low_iter = build_concat(l1_sstables.clone())?;
                let iter = TwoMergeIterator::create(up_iter, low_iter)?;
                self.compact_new_sst(iter)
            }
            CompactionTask::Simple(task) => {
                if task.upper_level.is_some() {
                    let up_iter = build_concat(task.upper_level_sst_ids.clone())?;
                    let low_iter = build_concat(task.lower_level_sst_ids.clone())?;
                    let iter = TwoMergeIterator::create(up_iter, low_iter)?;
                    self.compact_new_sst(iter)
                } else {
                    let up_iter = build_sst(task.upper_level_sst_ids.clone())?;
                    let low_iter = build_concat(task.lower_level_sst_ids.clone())?;
                    let iter = TwoMergeIterator::create(up_iter, low_iter)?;
                    self.compact_new_sst(iter)
                }
            }
            CompactionTask::Tiered(task) => {
                let iters: Vec<SstConcatIterator> = task
                    .tiers
                    .iter()
                    .map(|(_, ids)| build_concat(ids.clone()))
                    .collect::<Result<_>>()?;

                let iter = MergeIterator::create(iters.into_iter().map(Box::new).collect());
                self.compact_new_sst(iter)
            }
            _ => Ok(vec![]),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0, l1) = {
            let state = self.state.read();
            let l0 = state.l0_sstables.clone();
            let l1 = state.levels.first().unwrap_or(&(0, vec![])).1.clone();
            (l0, l1)
        };
        if l0.is_empty() && l1.is_empty() {
            return Ok(());
        }
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0.clone(),
            l1_sstables: l1.clone(),
        };
        let new_ssts = self.compact(&task)?;
        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.write().as_ref().clone();

            let mut l0_sstables_map = l0.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|x| !l0_sstables_map.remove(x))
                .copied()
                .collect::<Vec<_>>();

            let new_sst_ids: Vec<usize> = new_ssts.iter().map(|sst| sst.sst_id()).collect();
            for sst in new_ssts {
                state.sstables.insert(sst.sst_id(), sst.clone());
            }
            if !state.levels.is_empty() {
                state.levels[0].1 = new_sst_ids;
            } else {
                state.levels.push((0, new_sst_ids));
            }

            for id in l0.iter().chain(l1.iter()) {
                let result = state.sstables.remove(id);
                assert!(result.is_some(), "cannot remove {}.sst", id);
            }

            for &id in l0.iter().chain(l1.iter()) {
                std::fs::remove_file(self.path_of_sst(id))?;
            }
            *self.state.write() = Arc::new(state);
        };
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let task = self
            .compaction_controller
            .generate_compaction_task(snapshot.as_ref());
        if task.is_none() {
            return Ok(());
        }

        let task = task.unwrap();
        let new_sst = self.compact(&task)?;

        let remove_sst = {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.write().as_ref().clone();
            let mut new_ids = Vec::with_capacity(new_sst.len());
            for sst in new_sst {
                new_ids.push(sst.sst_id());
                state.sstables.insert(sst.sst_id(), sst.clone());
            }
            let (mut state, remove_ids) = self.compaction_controller.apply_compaction_result(
                &state,
                &task,
                new_ids.as_slice(),
                false,
            );
            let mut remove_sst = Vec::with_capacity(remove_ids.len());

            for id in remove_ids {
                let result = state.sstables.remove(&id);
                assert!(result.is_some(), "cannot remove {}.sst", id);
                remove_sst.push(result.unwrap());
            }

            *self.state.write() = Arc::new(state);

            remove_sst
        };

        for sst in remove_sst {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?;
        }

        Ok(())
    }

    fn compact_new_sst(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut result: Vec<Arc<SsTable>> = Vec::new();
        let mut new_sst = SsTableBuilder::new(self.options.block_size);
        while iter.is_valid() {
            let key = iter.key();
            let value = iter.value();
            if !value.is_empty() {
                new_sst.add(key, value);
                if new_sst.estimated_size() >= self.options.target_sst_size {
                    let sst_id = self.next_sst_id();
                    let sst = new_sst.build(
                        sst_id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(sst_id),
                    )?;
                    result.push(Arc::new(sst));
                    new_sst = SsTableBuilder::new(self.options.block_size);
                }
            }
            let _ = iter.next();
        }
        if new_sst.estimated_size() > 0 {
            let sst_id = self.next_sst_id();
            let sst_path = self.path_of_sst(sst_id);
            let sst = new_sst.build(sst_id, Some(self.block_cache.clone()), sst_path)?;
            result.push(Arc::new(sst));
        }
        Ok(result)
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let go = {
            let state = self.state.read();
            state.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if go {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
