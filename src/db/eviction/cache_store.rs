use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use fxhash::FxHasher;
use std::hash::{Hash, Hasher};
use tokio::sync::RwLock;

use crate::{config::EvictionType, types::ValueEntry};
use crate::db::eviction::{
    EvictionPolicy, LfuNode, LruNode,
};
use crate::db::eviction::direct_node::DirectCacheNode;
use crate::db::eviction::lua_node::LuaCacheNode;
use crate::db::LockedDb;

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
    pub evicition: std::sync::Mutex<Box<dyn EvictionPolicy>>,
}

impl MemoryCacheNode {
    pub fn new(config_type: &EvictionType) -> Self {
        let policy_instance: Box<dyn EvictionPolicy> = match config_type {
            EvictionType::LRU => Box::new(LruNode::new()),
            EvictionType::LFU => Box::new(LfuNode::new()),
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
