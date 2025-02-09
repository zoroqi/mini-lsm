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

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if self.options.max_levels == 0 {
            return None;
        }
        let l0 = _snapshot.l0_sstables.len();
        if l0 >= self.options.level0_file_num_compaction_trigger {
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: 1,
                lower_level_sst_ids: _snapshot.levels[0].1.clone(),
                is_lower_level_bottom_level: false,
            });
        }
        for i in 0..self.options.max_levels - 1 {
            let l_upper = _snapshot.levels[i].1.len();
            let l_lower = _snapshot.levels[i + 1].1.len();
            let ratio = (l_lower as f64 / l_upper as f64) * 100.0;
            println!(
                "l_upper {} l_lower {} ratio {} size_ratio {}",
                l_upper, l_lower, ratio, self.options.size_ratio_percent
            );
            if ratio < self.options.size_ratio_percent as f64 {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(i + 1),
                    upper_level_sst_ids: _snapshot.levels[i].1.clone(),
                    lower_level: i + 2,
                    lower_level_sst_ids: _snapshot.levels[i + 1].1.clone(),
                    is_lower_level_bottom_level: l_upper == self.options.max_levels - 1,
                });
            }
        }
        None
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut remove_sst_ids = Vec::new();
        if _task.upper_level.is_none() {
            remove_sst_ids.extend(_task.upper_level_sst_ids.clone());
            let mut l0_sstables_map = _task
                .upper_level_sst_ids
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            snapshot.l0_sstables = snapshot
                .l0_sstables
                .iter()
                .filter(|x| !l0_sstables_map.remove(x))
                .copied()
                .collect::<Vec<_>>();
        } else {
            let upper = _task.upper_level.unwrap() - 1;
            remove_sst_ids.extend(_task.upper_level_sst_ids.clone());
            snapshot.levels[upper].1.clear();
        }
        remove_sst_ids.extend(_task.lower_level_sst_ids.clone());
        snapshot.levels[_task.lower_level - 1].1 = _output.to_vec();
        (snapshot, remove_sst_ids)
    }
}
