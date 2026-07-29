pub mod skip_list;

use bytes::Bytes;
use skip_list::SkipList;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct ZSet {
    pub dict: HashMap<Bytes, f64>,
    pub zsl: SkipList,
}

impl ZSet {
    pub fn new() -> Self {
        ZSet {
            dict: HashMap::new(),
            zsl: SkipList::new(),
        }
    }

    /// Adds or updates a member's score. Returns true if a new member was added.
    pub fn insert(&mut self, score: f64, member: Bytes) -> bool {
        if let Some(old_score) = self.dict.get(&member) {
            if *old_score == score {
                return false; // No change needed
            }
            // Score changed, update skip list
            self.zsl.delete(*old_score, &member);
            self.zsl.insert(score, member.clone());
            self.dict.insert(member, score);
            false
        } else {
            self.zsl.insert(score, member.clone());
            self.dict.insert(member, score);
            true
        }
    }

    pub fn delete(&mut self, member: &Bytes) -> bool {
        if let Some(score) = self.dict.remove(member) {
            self.zsl.delete(score, member);
            true
        } else {
            false
        }
    }

    pub fn score(&self, member: &Bytes) -> Option<f64> {
        self.dict.get(member).copied()
    }

    pub fn rank(&self, member: &Bytes) -> Option<usize> {
        if let Some(score) = self.dict.get(member) {
            self.zsl.get_rank(*score, member)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.dict.len()
    }

    pub fn range(&self, start: isize, stop: isize) -> Vec<(Bytes, f64)> {
        let len = self.len() as isize;
        let start = if start < 0 { start + len } else { start };
        let stop = if stop < 0 { stop + len } else { stop };

        let start = start.max(0) as usize;
        let stop = stop.max(0) as usize;

        if start > stop || start >= self.len() {
            return Vec::new();
        }

        let stop = stop.min(self.len() - 1);
        let mut result = Vec::with_capacity(stop - start + 1);

        for i in start..=stop {
            if let Some((score, member)) = self.zsl.get_element_by_rank(i) {
                result.push((member, score));
            } else {
                break;
            }
        }
        result
    }

    /// Rough calculation of heap memory usage
    pub fn heap_memory_size(&self) -> usize {
        let dict_size =
            self.dict.capacity() * (std::mem::size_of::<Bytes>() + std::mem::size_of::<f64>() + 8);
        let zsl_size = self.zsl.nodes.capacity() * std::mem::size_of::<skip_list::Node>();
        dict_size + zsl_size
    }
}
