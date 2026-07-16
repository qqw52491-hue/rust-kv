use rand::Rng;
use std::{collections::HashMap, ptr::NonNull, sync::Arc};

use crate::db::eviction::{
    EvictionPolicy,
    lru::lru_linklist::{LruList, Node},
};

pub struct LfuNode {
    pub buckets: Vec<LruList>,         // 256 个桶 (0-255)，索引即频率
    pub sample_keys: Vec<Arc<String>>, // O(1) 采样数组
    pub map_key: HashMap<Arc<String>, LfuMetaPointers>,
    pub min_freq: usize,
}

unsafe impl Send for LfuNode {}
unsafe impl Sync for LfuNode {}

#[derive(Debug, Clone)]
pub struct LfuMetaPointers {
    pub freq: usize,
    pub lru_node: NonNull<Node>, // 指向 LRU 链表节点
    pub sample_idx: usize,       // 指向 Vec<Key> 的索引
}

impl LfuNode {
    pub fn new() -> Self {
        let mut buckets = Vec::with_capacity(256);
        for _ in 0..256 {
            buckets.push(LruList::new());
        }
        LfuNode {
            buckets,
            sample_keys: Vec::new(),
            map_key: HashMap::new(),
            min_freq: 1, // 新插入的数据默认频率为 1
        }
    }

    // 辅助函数：当一个桶空了之后，向上寻找下一个非空桶来更新 min_freq
    fn update_min_freq(&mut self) {
        while self.min_freq < 255 && self.buckets[self.min_freq].is_empty() {
            self.min_freq += 1;
        }
        // 如果连最高频 255 都空了，说明整个 LFU 空了，重置回 1
        if self.min_freq == 255 && self.buckets[255].is_empty() {
            self.min_freq = 1;
        }
    }
}

impl EvictionPolicy for LfuNode {
    fn on_write(&mut self, key: Arc<String>) {
        if !self.map_key.contains_key(&key) {
            let freq = 1;
            // 放入频率为 1 的桶的尾部
            let node_ptr = self.buckets[freq].push_back(key.clone());

            let index = self.sample_keys.len();
            self.sample_keys.push(key.clone());

            self.map_key.insert(
                key,
                LfuMetaPointers {
                    freq,
                    lru_node: node_ptr,
                    sample_idx: index,
                },
            );
            // 新元素加入，最低频率立刻降到 1
            self.min_freq = 1;
        } else {
            // 如果已经存在，写入操作也可以看作一次访问，频次需要增加
            self.on_read(&key);
        }
    }

    fn on_read(&mut self, key: &Arc<String>) {
        if let Some(mut meta) = self.map_key.get(key).cloned() {
            let old_freq = meta.freq;
            let mut new_freq = old_freq + 1;

            // 设定最大频率上限为 255
            if new_freq > 255 {
                new_freq = 255;
            }

            if new_freq != old_freq {
                // 1. 从旧桶拔出
                self.buckets[old_freq].pop_node(meta.lru_node);

                // 2. 放入新桶尾部
                let new_node_ptr = self.buckets[new_freq].push_back(key.clone());

                // 3. 更新元数据
                meta.freq = new_freq;
                meta.lru_node = new_node_ptr;
                self.map_key.insert(key.clone(), meta);

                // 4. 维护 min_freq
                if self.min_freq == old_freq && self.buckets[old_freq].is_empty() {
                    self.update_min_freq();
                }
            } else {
                // 如果已经达到上限 255，就在 255 的桶里执行 LRU 逻辑（移到队尾）
                self.buckets[new_freq].push_mid_back(meta.lru_node);
            }
        }
    }

    fn on_delete(&mut self, key: Arc<String>) {
        if let Some(meta_ptr) = self.map_key.remove(&key) {
            // 从所在的频率桶中删除
            self.buckets[meta_ptr.freq].pop_node(meta_ptr.lru_node);

            // 从采样 Vec 中删除
            let idx_to_remove = meta_ptr.sample_idx;
            self.sample_keys.swap_remove(idx_to_remove);

            let moved_key_cloned = self.sample_keys.get(idx_to_remove).cloned();

            if let Some(k) = moved_key_cloned {
                if let Some(moved_meta) = self.map_key.get_mut(&k) {
                    moved_meta.sample_idx = idx_to_remove;
                }
            }

            // 维护 min_freq
            if self.min_freq == meta_ptr.freq && self.buckets[meta_ptr.freq].is_empty() {
                self.update_min_freq();
            }
        }
    }

    fn get_random_sample_key(&self) -> Option<Arc<String>> {
        if self.sample_keys.is_empty() {
            return None;
        }
        let random_active_index = rand::thread_rng().gen_range(0..self.sample_keys.len());
        Some(self.sample_keys.get(random_active_index).cloned().unwrap())
    }

    fn pop_victim(&mut self) -> Option<Arc<String>> {
        // 二次校验，防止某些极端情况下 min_freq 桶为空
        if self.buckets[self.min_freq].is_empty() {
            self.update_min_freq();
        }
        // 在最低频率桶中，弹出最久未访问（最头部）的节点
        self.buckets[self.min_freq].peek_front()
    }
}
