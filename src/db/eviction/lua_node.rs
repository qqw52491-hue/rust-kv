use std::collections::HashMap;
use std::sync::Arc;

use crate::types::ValueEntry;
use crate::db::eviction::traits::{KvOperator, Transactional};
use crate::db::eviction::direct_node::DirectCacheNode;

//lua 变更级数据源模拟
// 定义一个包装类型
pub enum ChangeOp {
    Update(ValueEntry),
    Delete,
}

// 包装器代理
pub struct LuaCacheNode {
    pub db_store: DirectCacheNode,
    pub differ_map: HashMap<Arc<String>, ChangeOp>,
    pub local_memory_diff: isize,
}

impl LuaCacheNode {
    pub fn new(db_store: DirectCacheNode) -> Self {
        LuaCacheNode {
            db_store,
            differ_map: HashMap::new(),
            local_memory_diff: 0,
        }
    }
}

impl KvOperator for LuaCacheNode {
    fn insert(&mut self, key: Arc<String>, value: ValueEntry) {
        let size_before = match self.select(&key) {
            Some(entry) => entry.data_size,
            None => 0,
        };
        //插入修改类别的 都是覆盖 如果没有就插入
        let memory_differ = value.data_size as isize - size_before as isize;
        self.differ_map.insert(key, ChangeOp::Update(value));
        self.local_memory_diff += memory_differ;
    }

    fn select(&mut self, key: &Arc<String>) -> Option<&ValueEntry> {
        match self.differ_map.get(key) {
            Some(change) => match change {
                ChangeOp::Update(value_entry) => Some(value_entry),
                ChangeOp::Delete => None,
            },
            None => self.db_store.select(key),
        }
    }

    fn take(&mut self, key: &Arc<String>) -> Option<ValueEntry> {
        if let Some(change) = self.differ_map.remove(key) {
            match change {
                ChangeOp::Update(value_entry) => {
                    self.local_memory_diff -= value_entry.data_size as isize;
                    self.differ_map.insert(key.clone(), ChangeOp::Delete);
                    return Some(value_entry);
                }
                ChangeOp::Delete => {
                    self.differ_map.insert(key.clone(), ChangeOp::Delete);
                    return None;
                }
            }
        }

        if let Some(value_entry) = self.db_store.select(key) {
            let cloned_entry = value_entry.clone();
            self.differ_map.insert(key.clone(), ChangeOp::Delete);
            self.local_memory_diff -= cloned_entry.data_size as isize;
            Some(cloned_entry)
        } else {
            None
        }
    }

    //说明一下 这个usize 转 isize 就是在小于800万TB都是没问题  位数足够大 一般不会超过这个的感觉
    fn delete(&mut self, key: &Arc<String>) {
        let size_before = match self.select(&key) {
            Some(entry) => entry.data_size,
            None => 0,
        };
        self.differ_map.insert(key.clone(), ChangeOp::Delete);
        self.local_memory_diff -= size_before as isize;
    }
}

impl Transactional for LuaCacheNode {
    fn commit(&mut self) {
        for (key, change) in self.differ_map.drain() {
            match change {
                ChangeOp::Update(value_entry) => {
                    self.db_store.insert(key, value_entry);
                }
                ChangeOp::Delete => {
                    self.db_store.delete(&key);
                }
            }
        }
    }
}
