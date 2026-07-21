use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use fxhash::FxHasher;
use std::hash::{Hash, Hasher};
use tokio::sync::RwLock;

use crate::db::LockedDb;
use crate::db::eviction::direct_node::DirectCacheNode;
use crate::db::eviction::lua_node::LuaCacheNode;
use crate::db::eviction::{EvictionPolicy, LfuNode, LruNode};
use crate::{config::EvictionType, types::ValueEntry};

pub const NUM_SHARDS: usize = 64; // 64 个分片

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct TtlEntry {
    pub expires_at: u64,
    pub key: Arc<String>,
}

pub static GLOBAL_MEMORY: AtomicUsize = AtomicUsize::new(0);



#[repr(align(64))]
pub struct MemoryCacheNode {
    pub db_store: HashMap<Arc<String>, ValueEntry>,
    pub approx_memory: AtomicUsize, // 它自己分片的账 记录具体的内存大小
    pub evicition: std::sync::Mutex<crate::db::eviction::strategy::EvictionStrategy>,
}

impl MemoryCacheNode {
    pub fn new(config_type: &EvictionType) -> Self {
        let policy_instance = match config_type {
            EvictionType::LRU => crate::db::eviction::strategy::EvictionStrategy::Lru(LruNode::new()),
            EvictionType::LFU => crate::db::eviction::strategy::EvictionStrategy::Lfu(LfuNode::new()),
        };
        MemoryCacheNode {
            db_store: HashMap::new(),
            approx_memory: AtomicUsize::new(0),
            evicition: std::sync::Mutex::new(policy_instance),
        }
    }

    pub fn get_memory_usage(&self) -> usize {
        self.approx_memory.load(Ordering::Relaxed)
    }

    /// 核心逻辑封装：插入数据时，自动处理淘汰策略和内存统计
    pub fn insert_entry(&mut self, key: Arc<String>, value: ValueEntry) {
        // 1. 触发淘汰策略记录写入
        self.evicition.lock().unwrap().on_write(key.clone());

        let size_before = match self.db_store.get(&key) {
            Some(entry) => entry.data_size,
            None => 0,
        };

        let memory_differ = value.data_size as isize - size_before as isize;

        // 2. 真实插入数据
        self.db_store.insert(key, value);

        // 3. 同步更新内存账本
        if memory_differ > 0 {
            self.approx_memory.fetch_add(memory_differ as usize, Ordering::Relaxed);
            GLOBAL_MEMORY.fetch_add(memory_differ as usize, Ordering::Relaxed);
        } else if memory_differ < 0 {
            self.approx_memory.fetch_sub((-memory_differ) as usize, Ordering::Relaxed);
            GLOBAL_MEMORY.fetch_sub((-memory_differ) as usize, Ordering::Relaxed);
        }
    }

    /// 核心逻辑封装：删除数据时，自动触发淘汰策略和释放内存
    pub fn remove_entry(&mut self, key: &Arc<String>) -> Option<ValueEntry> {
        if let Some(value) = self.db_store.remove(key) {
            // 触发淘汰策略记录删除
            self.evicition.lock().unwrap().on_delete(key.clone());
            // 释放内存
            self.approx_memory.fetch_sub(value.data_size, Ordering::Relaxed);
            GLOBAL_MEMORY.fetch_sub(value.data_size, Ordering::Relaxed);
            Some(value)
        } else {
            None
        }
    }

    /// 核心逻辑封装：读取数据，并自动触发淘汰策略的 `on_read`
    pub fn get_entry(&mut self, key: &Arc<String>) -> Option<&ValueEntry> {
        if let Some(value) = self.db_store.get(key) {
            self.evicition.lock().unwrap().on_read(key);
            Some(value)
        } else {
            None
        }
    }

    /// 仅做单纯读取，不触发可变的 `on_read`（用于纯读锁场景）
    pub fn peek_entry(&self, key: &Arc<String>) -> Option<&ValueEntry> {
        self.db_store.get(key)
    }
}

#[derive(Default, Clone)]
pub struct MemoryCache {
    pub message: Vec<Arc<RwLock<MemoryCacheNode>>>,
}

impl MemoryCache {
    pub fn new(config_type: &EvictionType) -> Self {
        let mut local_vec: Vec<Arc<RwLock<MemoryCacheNode>>> = Vec::with_capacity(NUM_SHARDS);
        for _ in 0..NUM_SHARDS {
            local_vec.push(Arc::new(RwLock::new(MemoryCacheNode::new(config_type))));
        }
        MemoryCache { message: local_vec }
    }

    pub fn get_shard_index<K: Hash>(key: &K) -> usize {
        let mut hasher = FxHasher::default();
        key.hash(&mut hasher);
        let hash_value = hasher.finish();
        (hash_value as usize) % NUM_SHARDS
    }

    pub async fn get_lock_write(&self, key: &Arc<String>) -> LockedDb {
        let shard_index = MemoryCache::get_shard_index(&key);
        let shard = self.message[shard_index].clone().write_owned().await;
        LockedDb::WriteNormal(DirectCacheNode::Writeguard(shard))
    }

    pub async fn lock_write_lua(&self, key: &Arc<String>) -> (LockedDb, usize) {
        let shard_index = MemoryCache::get_shard_index(&key);
        let shard = self.message[shard_index].clone().write_owned().await;
        (
            LockedDb::WriteLua(LuaCacheNode::new(DirectCacheNode::Writeguard(shard))),
            shard_index,
        )
    }

    pub async fn get_lua_lock_write_shard_index(&self, shard_index: usize) -> LockedDb {
        let shard = self.message[shard_index].clone().write_owned().await;
        LockedDb::WriteLua(LuaCacheNode::new(DirectCacheNode::Writeguard(shard)))
    }

    pub async fn get_lock_write_shard_index(&self, shard_index: usize) -> LockedDb {
        let shard = self.message[shard_index].clone().write_owned().await;
        LockedDb::WriteNormal(DirectCacheNode::Writeguard(shard))
    }

    pub async fn get_lock_read(&self, key: &Arc<String>) -> LockedDb {
        let shard_index = MemoryCache::get_shard_index(&key);
        let shard = self.message[shard_index].clone().read_owned().await;
        LockedDb::ReadNormal(DirectCacheNode::Readguard(shard))
    }

    pub async fn get_lock_read_shard_index(&self, shard_index: usize) -> LockedDb {
        let shard = self.message[shard_index].clone().read_owned().await;
        LockedDb::ReadNormal(DirectCacheNode::Readguard(shard))
    }

    pub async fn lock_read_lua(&self, key: &Arc<String>) -> (LockedDb, usize) {
        let shard_index = MemoryCache::get_shard_index(&key);
        let shard = self.message[shard_index].clone().read_owned().await;
        (
            LockedDb::ReadLua(LuaCacheNode::new(DirectCacheNode::Readguard(shard))),
            shard_index,
        )
    }
}
