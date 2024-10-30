use std::{iter, os::linux::raw::stat};

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;
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
        let mut ret = Vec::new();
        let mut upper_level_first_key = snapshot.sstables[sst_ids.first().unwrap()]
            .first_key()
            .clone();
        for i in sst_ids {
            upper_level_first_key =
                upper_level_first_key.min(snapshot.sstables[i].first_key().clone())
        }
        let mut upper_level_last_key = snapshot.sstables[sst_ids.first().unwrap()]
            .last_key()
            .clone();
        for i in sst_ids {
            upper_level_last_key = upper_level_last_key.max(snapshot.sstables[i].last_key().clone())
        }

        for i in &snapshot.levels[in_level - 1].1 {
            let sst = &snapshot.sstables[i];
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            if !(first_key > &upper_level_last_key || last_key < &upper_level_first_key) {
                ret.push(*i);
            }
        }
        ret
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // 1. calaute level size
        let mut level_sizes: Vec<usize> = Vec::new();

        for i in 0..self.options.max_levels {
            let mut level_size = 0;

            for sst_id in snapshot.levels[i].1.iter() {
                level_size += snapshot.sstables.get(sst_id).unwrap().table_size() as usize;
            }
            level_sizes.push(level_size);
        }
        let base_level_size = self.options.base_level_size_mb * 1024 * 1024;
        // 2. calaute target size
        let mut target_size: Vec<usize> =
            (0..self.options.max_levels).map(|_| 0).collect::<Vec<_>>();
        target_size[self.options.max_levels - 1] =
            level_sizes[self.options.max_levels - 1].max(base_level_size);

        for i in (1..self.options.max_levels).rev() {
            let next = target_size[i];
            let cur = next / self.options.level_size_multiplier;
            if next > base_level_size {
                target_size[i - 1] = cur;
            }
        }
        // 3. calaute target level
        let mut target_level = self.options.max_levels;
        for i in 1..self.options.max_levels {
            if target_size[i - 1] > 0 {
                target_level = i;
                break;
            }
        }
        // 4 L0 compaction
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: target_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    target_level,
                ),
                is_lower_level_bottom_level: target_level == self.options.max_levels,
            });
        }
        // 5. calaute max ratio and max ratio level
        let mut max_ratio: f64 = 0.0;
        let mut max_ratio_level = 0;
        for level in 0..self.options.max_levels - 1 {
            let ratio = level_sizes[level] as f64 / target_size[level] as f64;
            if ratio > max_ratio {
                max_ratio = ratio;
                max_ratio_level = level;
            }
        }
        if max_ratio < 1.0 {
            return None;
        }
        let target_sst = snapshot.levels[max_ratio_level]
            .1
            .iter()
            .min()
            .copied()
            .unwrap();
        // 6.other compaction
        return Some(LeveledCompactionTask {
            upper_level: Some(max_ratio_level + 1),
            upper_level_sst_ids: vec![target_sst],
            lower_level: max_ratio_level + 2,
            lower_level_sst_ids: self.find_overlapping_ssts(
                snapshot,
                &[target_sst],
                max_ratio_level + 2,
            ),
            is_lower_level_bottom_level: max_ratio_level + 1 == self.options.max_levels - 1,
        });
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut state = snapshot.clone();
        let mut del: Vec<usize> = vec![];

        let delete_upper_sst_id = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if task.upper_level.is_none() {
            let mut l0_ssts = vec![];
            for id in state.l0_sstables.iter() {
                if !delete_upper_sst_id.contains(&id) {
                    l0_ssts.push(*id);
                }
            }
            state.l0_sstables = l0_ssts;
        } else {
            let mut upper_ssts = vec![];
            for id in state.levels[task.upper_level.unwrap() - 1].1.iter() {
                if !delete_upper_sst_id.contains(&id) {
                    upper_ssts.push(*id);
                }
            }
            state.levels[task.upper_level.unwrap() - 1].1 = upper_ssts;
        }

        let mut lower_ssts = vec![];
        let delete_lower_sst_id = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        for id in snapshot.levels[task.lower_level - 1].1.iter() {
            if !delete_lower_sst_id.contains(&id) {
                lower_ssts.push(*id);
            }
        }
        lower_ssts.extend(output);
        lower_ssts.sort_by(|a, b| {
            snapshot
                .sstables
                .get(a)
                .unwrap()
                .first_key()
                .cmp(snapshot.sstables.get(b).unwrap().first_key())
        });
        state.levels[task.lower_level - 1].1 = lower_ssts;
        del.extend(&task.upper_level_sst_ids);
        del.extend(&task.lower_level_sst_ids);

        (state, del)
    }
}
