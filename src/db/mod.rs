use bytes::Bytes;
use itoa::Buffer;
use std::sync::Arc;
pub mod eviction;
mod generic;
mod hash;
mod list;
mod string;
pub mod zset;

use crate::{
    config::EvictionType,
    context::CONN_STATE,
    db::eviction::{
        DirectCacheNode, EvictionPolicy, KvOperator, LockOwner, LuaCacheNode, MemoryCache,
        Transactional,
    },
    types::ValueEntry,
};

#[derive(Clone)]
pub struct Db {
    pub store: Storage,
}
impl Db {
    pub fn new(config_type: &EvictionType) -> Self {
        Self {
            store: Storage::new(config_type),
        }
    }
}

pub enum LockedDb {
    WriteNormal(DirectCacheNode),
    WriteLua(LuaCacheNode),
    ReadNormal(DirectCacheNode),
    ReadLua(LuaCacheNode),
}

impl KvOperator for LockedDb {
    fn insert(&mut self, key: Arc<String>, value: ValueEntry) {
        match self {
            LockedDb::WriteNormal(node) => node.insert(key, value),
            LockedDb::WriteLua(node) => node.insert(key, value),
            _ => panic!("Cannot insert on a read lock"),
        }
    }

    fn select(&mut self, key: &Arc<String>) -> Option<&ValueEntry> {
        match self {
            LockedDb::WriteNormal(node) => node.select(key),
            LockedDb::WriteLua(node) => node.select(key),
            LockedDb::ReadNormal(node) => node.select(key),
            LockedDb::ReadLua(node) => node.select(key),
        }
    }

    fn take(&mut self, key: &Arc<String>) -> Option<ValueEntry> {
        match self {
            LockedDb::WriteNormal(node) => node.take(key),
            LockedDb::WriteLua(node) => node.take(key),
            _ => panic!("Cannot take on a read lock"),
        }
    }

    fn delete(&mut self, key: &Arc<String>) {
        match self {
            LockedDb::WriteNormal(node) => node.delete(key),
            LockedDb::WriteLua(node) => node.delete(key),
            _ => panic!("Cannot delete on a read lock"),
        }
    }
}

impl Transactional for LockedDb {
    fn commit(&mut self) {
        if let LockedDb::WriteLua(node) = self {
            node.commit();
        }
    }
}

impl LockOwner for LockedDb {
    fn get_memory_usage(&self) -> usize {
        match self {
            LockedDb::WriteNormal(node) | LockedDb::ReadNormal(node) => node.get_memory_usage(),
            LockedDb::WriteLua(node) | LockedDb::ReadLua(node) => node.db_store.get_memory_usage(),
        }
    }

    fn get_eviction_policy(&self) -> Option<std::sync::MutexGuard<'_, Box<dyn EvictionPolicy>>> {
        match self {
            LockedDb::WriteNormal(node) | LockedDb::ReadNormal(node) => node.get_eviction_policy(),
            LockedDb::WriteLua(node) | LockedDb::ReadLua(node) => {
                node.db_store.get_eviction_policy()
            }
        }
    }

    fn add_memory(&self, size: usize) {
        match self {
            LockedDb::WriteNormal(node) | LockedDb::ReadNormal(node) => node.add_memory(size),
            LockedDb::WriteLua(node) | LockedDb::ReadLua(node) => node.db_store.add_memory(size),
        }
    }

    fn sub_memory(&self, size: usize) {
        match self {
            LockedDb::WriteNormal(node) | LockedDb::ReadNormal(node) => node.sub_memory(size),
            LockedDb::WriteLua(node) | LockedDb::ReadLua(node) => node.db_store.sub_memory(size),
        }
    }
}

#[derive(Clone, Default)]
pub struct Storage {
    pub(crate) store: Arc<Vec<Arc<MemoryCache>>>,
    // 阻塞队列通知中心: HashMap<DB_Index, HashMap<Key, VecDeque<Sender>>>
    pub(crate) blocking_queues: Arc<
        Vec<
            tokio::sync::Mutex<
                std::collections::HashMap<
                    Arc<String>,
                    std::collections::VecDeque<tokio::sync::oneshot::Sender<bytes::Bytes>>,
                >,
            >,
        >,
    >,
}

impl Storage {
    pub fn new(config_type: &EvictionType) -> Self {
        let mut local_vec: Vec<Arc<MemoryCache>> = Vec::with_capacity(16);
        let mut queues_vec = Vec::with_capacity(16);
        for _ in 0..16 {
            local_vec.push(Arc::new(MemoryCache::new(config_type)));
            queues_vec.push(tokio::sync::Mutex::new(std::collections::HashMap::new()));
        }
        Storage {
            store: Arc::new(local_vec),
            blocking_queues: Arc::new(queues_vec),
        }
    }

    pub async fn lock_write(&self, key: &Arc<String>) -> LockedDb {
        let select_db = CONN_STATE.with(|state| state.selected_db);
        self.store.get(select_db).unwrap().get_lock_write(key).await
    }

    pub async fn lock_read(&self, key: &Arc<String>) -> LockedDb {
        let select_db = CONN_STATE.with(|state| state.selected_db);
        self.store.get(select_db).unwrap().get_lock_read(key).await
    }

    pub async fn lock_write_lua<'a>(&'a self, shard_index: usize) -> LockedDb {
        let select_db = CONN_STATE.with(|state| state.selected_db);
        self.store
            .get(select_db)
            .unwrap()
            .get_lua_lock_write_shard_index(shard_index)
            .await
    }

    pub async fn lock_read_lua<'a>(&'a self, shard_index: usize) -> LockedDb {
        let select_db = CONN_STATE.with(|state| state.selected_db);
        self.store
            .get(select_db)
            .unwrap()
            .get_lock_read_shard_index(shard_index)
            .await
    }

    pub async fn get_lock_write(&self, db_index: usize, shard_index: usize) -> LockedDb {
        self.store
            .get(db_index)
            .unwrap()
            .get_lock_write_shard_index(shard_index)
            .await
    }

    pub async fn get_lock_read(&self, db_index: usize, shard_index: usize) -> LockedDb {
        self.store
            .get(db_index)
            .unwrap()
            .clone()
            .get_lock_read_shard_index(shard_index)
            .await
    }
}

pub fn bytes_to_i64_fast(b: &Bytes) -> Option<i64> {
    let result = lexical_core::parse::<i64>(b);
    result.ok()
}

pub fn parse_int_from_bytes(i: i64) -> Bytes {
    let mut buffer = Buffer::new();
    let printed_str = buffer.format(i);
    Bytes::copy_from_slice(printed_str.as_bytes())
}
