use crate::lsm_storage::LsmStorageState;
use nom::character::complete::u64;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::HashSet;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        // find the oldest sst in the upper level, the least sst_id is the oldest
        let first_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();
        let last_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();

        snapshot.levels[in_level]
            .1
            .iter()
            .filter(|i| {
                let sst = snapshot.sstables[i].clone();
                sst.last_key() >= &first_key && sst.first_key() <= &last_key
            })
            .copied()
            .collect::<Vec<_>>()
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let level_cnt = self.options.max_levels;

        let mut real_size: Vec<u64> = vec![0; level_cnt];
        for i in 0..level_cnt {
            real_size[i] = _snapshot.levels[i]
                .1
                .iter()
                .map(|f| _snapshot.sstables[f].table_size())
                .sum::<u64>();
        }

        let base_size = (self.options.base_level_size_mb as u64) << 20;
        let mut target_size: Vec<u64> = vec![0; level_cnt];
        target_size[level_cnt - 1] = max(real_size[level_cnt - 1], base_size);
        for i in (0..level_cnt - 1).rev() {
            let next_level = target_size[i + 1];
            let this_level = next_level / self.options.level_size_multiplier as u64;
            if next_level > base_size {
                target_size[i] = this_level;
            }
        }

        // Task 1.2: Decide Base Level
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let target_level = target_size.iter().position(|&x| x > 0).unwrap();
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: target_level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    _snapshot,
                    &_snapshot.l0_sstables,
                    target_level,
                ),
                is_lower_level_bottom_level: target_level + 1 == level_cnt,
            });
        }

        // Task 1.3: Decide Level Priorities
        let mut priorities = Vec::with_capacity(level_cnt);
        for level in 0..level_cnt - 1 {
            let prio = real_size[level] as f64 / target_size[level] as f64;
            if prio > 1.0 {
                priorities.push((prio, level));
            }
        }

        priorities.sort_by(|a, b| a.partial_cmp(b).unwrap().reverse());
        if let Some((_, max_ratio_level)) = priorities.first() {
            let level = *max_ratio_level;
            let oldest_sst = _snapshot.levels[level].1.iter().min().copied().unwrap();
            let overlapping = self.find_overlapping_ssts(_snapshot, &[oldest_sst], level + 1);
            return Some(LeveledCompactionTask {
                upper_level: Some(level + 1),
                upper_level_sst_ids: vec![oldest_sst],
                lower_level: level + 2,
                lower_level_sst_ids: overlapping,
                is_lower_level_bottom_level: level + 2 == level_cnt,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut remove_sst_ids = Vec::new();
        remove_sst_ids.extend(_task.upper_level_sst_ids.clone());
        remove_sst_ids.extend(_task.lower_level_sst_ids.clone());

        let mut remove_ids_set = remove_sst_ids.iter().copied().collect::<HashSet<_>>();

        let mut remove = |ids: Vec<usize>| -> Vec<usize> {
            ids.iter()
                .filter(|x| !remove_ids_set.remove(x))
                .copied()
                .collect::<Vec<_>>()
        };
        if _task.upper_level.is_none() {
            snapshot.l0_sstables = remove(snapshot.l0_sstables);
        } else {
            let upper = _task.upper_level.unwrap() - 1;
            snapshot.levels[upper].1 = remove(snapshot.levels[upper].1.clone());
        }
        let lower = _task.lower_level - 1;
        let mut new_lower_level_ssts = remove(snapshot.levels[lower].1.clone());

        assert!(remove_ids_set.is_empty());

        new_lower_level_ssts.extend(_output);
        if !_in_recovery {
            new_lower_level_ssts.sort_by(|x, y| {
                snapshot
                    .sstables
                    .get(x)
                    .unwrap()
                    .first_key()
                    .cmp(snapshot.sstables.get(y).unwrap().first_key())
            });
        }

        snapshot.levels[lower].1 = new_lower_level_ssts;
        (snapshot, remove_sst_ids)
    }
}
