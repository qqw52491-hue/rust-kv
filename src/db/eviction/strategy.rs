use std::sync::Arc;
use crate::db::eviction::{EvictionPolicy, LfuNode, LruNode};

pub enum EvictionStrategy {
    Lru(LruNode),
    Lfu(LfuNode),
}

impl EvictionPolicy for EvictionStrategy {
    fn on_write(&mut self, key: Arc<String>) {
        match self {
            EvictionStrategy::Lru(node) => node.on_write(key),
            EvictionStrategy::Lfu(node) => node.on_write(key),
        }
    }

    fn on_read(&mut self, key: &Arc<String>) {
        match self {
            EvictionStrategy::Lru(node) => node.on_read(key),
            EvictionStrategy::Lfu(node) => node.on_read(key),
        }
    }

    fn on_delete(&mut self, key: Arc<String>) {
        match self {
            EvictionStrategy::Lru(node) => node.on_delete(key),
            EvictionStrategy::Lfu(node) => node.on_delete(key),
        }
    }

    fn get_random_sample_key(&self) -> Option<Arc<String>> {
        match self {
            EvictionStrategy::Lru(node) => node.get_random_sample_key(),
            EvictionStrategy::Lfu(node) => node.get_random_sample_key(),
        }
    }

    fn pop_victim(&mut self) -> Option<Arc<String>> {
        match self {
            EvictionStrategy::Lru(node) => node.pop_victim(),
            EvictionStrategy::Lfu(node) => node.pop_victim(),
        }
    }
}
