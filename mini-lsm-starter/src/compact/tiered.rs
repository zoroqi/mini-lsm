use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if _snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        let snapshot = _snapshot.clone();
        // Task 1.1: Triggered by Space Amplification Ratio
        let total_size = snapshot
            .levels
            .iter()
            .map(|(_, files)| files.len())
            .sum::<usize>();
        let last_size = snapshot.levels.last().unwrap().1.len();
        let space_amplification_ratio =
            ((total_size - last_size) as f64 / last_size as f64) * 100.0;
        if space_amplification_ratio >= self.options.max_size_amplification_percent as f64 {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // Task 1.2: Triggered by Size Ratio
        let mut upper_total: f64 = 0.0;
        for i in 1..snapshot.levels.len() {
            let upper = snapshot.levels[i - 1].1.len() as f64;
            upper_total += upper;
            let lower = snapshot.levels[i].1.len() as f64;
            let ratio = (lower / upper_total) * 100.0;
            if ratio >= (self.options.size_ratio + 100) as f64
                && upper_total >= self.options.min_merge_width as f64
            {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels[0..i].to_vec(),
                    bottom_tier_included: i + 1 >= snapshot.levels.len(),
                });
            }
        }

        // Task 1.3: Reduce Sorted Runs
        let merge_with = snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));
        Some(TieredCompactionTask {
            tiers: snapshot.levels[0..merge_with].to_vec(),
            bottom_tier_included: merge_with >= snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let mut removed_ids = Vec::new();
        let mut map = std::collections::HashSet::new();
        for (level, ids) in _task.tiers.iter() {
            map.insert(level);
            removed_ids.extend(ids.clone())
        }

        let first_tier_id = _task.tiers.first().unwrap().0;
        let first_index = snapshot
            .levels
            .iter()
            .position(|(tier_id, _)| first_tier_id == *tier_id)
            .unwrap();

        let mut new_levels: Vec<(usize, Vec<usize>)> = snapshot
            .levels
            .into_iter()
            .filter(|(tire_id, _)| !map.contains(tire_id))
            .collect();

        new_levels.insert(first_index, (_output[0], _output.to_vec()));

        snapshot.levels = new_levels;

        (snapshot, removed_ids.to_vec())
    }
}
