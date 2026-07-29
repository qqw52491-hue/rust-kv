use std::sync::{Arc, atomic::Ordering};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard};

use crate::core_time::get_cached_time_ms;
use crate::db::eviction::cache_store::{GLOBAL_MEMORY, MemoryCacheNode};
use crate::db::eviction::traits::{EvictionPolicy, KvOperator, LockOwner};
use crate::types::ValueEntry;

// 场景 A: 普通模式的包装器
// 它只负责持有锁，操作直接透传给底层
pub enum DirectCacheNode {
    // 这里持有 map 过的锁
    Writeguard(OwnedRwLockWriteGuard<MemoryCacheNode>),
    Readguard(OwnedRwLockReadGuard<MemoryCacheNode>),
}

impl KvOperator for DirectCacheNode {
    fn insert(&mut self, key: Arc<String>, value: ValueEntry) {
        match self {
            DirectCacheNode::Writeguard(rw_lock_write_guard) => {
                // 所有的内存统计、淘汰策略触发都被封装进了 insert_entry
                rw_lock_write_guard.insert_entry(key, value);
            }
            DirectCacheNode::Readguard(_rw_lock_read_guard) => {}
        }
    }

    fn delete(&mut self, key: &Arc<String>) {
        match self {
            DirectCacheNode::Writeguard(rw_lock_write_guard) => {
                // 封装了底层删除，以及同步更新内存和 LRU 链表的逻辑
                rw_lock_write_guard.remove_entry(key);
            }
            DirectCacheNode::Readguard(_rw_lock_read_guard) => {}
        }
    }

    fn take(&mut self, key: &Arc<String>) -> Option<ValueEntry> {
        match self {
            DirectCacheNode::Writeguard(rw_lock_write_guard) => {
                rw_lock_write_guard.remove_entry(key)
            }
            DirectCacheNode::Readguard(_rw_lock_read_guard) => None,
        }
    }

    /*
    读写在内核代理层就完成
     */
    fn select(&mut self, key: &Arc<String>) -> Option<&ValueEntry> {
        match self {
            DirectCacheNode::Writeguard(guard) => {
                let node = &mut **guard;

                // 1. 先用 peek 进行纯读检查，确认是否过期
                let is_expired = match node.peek_entry(key) {
                    Some(value) => value
                        .expires_at
                        .is_some_and(|expires_at| get_cached_time_ms() > expires_at),
                    None => return None,
                };

                // 2. 如果过期，执行封装好的删除逻辑（它会自动维护内存和淘汰策略）
                if is_expired {
                    node.remove_entry(key);
                    return None;
                }

                // 3. 正常命中，调用 get_entry，它内部会自动调用 evicition.on_read
                node.get_entry(key)
            }
            DirectCacheNode::Readguard(rw_lock_read_guard) => {
                // 读锁可以判断过期，但不能修改 db_store 做物理删除。
                // 读路径只能返回 None，待未来的写请求或后台任务清理。
                let node = &**rw_lock_read_guard;
                let value = node.peek_entry(key)?;

                if value
                    .expires_at
                    .is_some_and(|expires_at| get_cached_time_ms() > expires_at)
                {
                    return None;
                }

                // 因为是 Readguard，不能调用可变的 node.get_entry()，只能手动通知淘汰策略
                // 这是一个妥协，读锁无法避免对 eviction 进行内部可变性修改
                node.evicition.lock().unwrap().on_read(key);
                Some(value)
            }
        }
    }
}

impl LockOwner for DirectCacheNode {
    fn get_memory_usage(&self) -> usize {
        match self {
            DirectCacheNode::Writeguard(rw_lock_write_guard) => {
                rw_lock_write_guard.approx_memory.load(Ordering::Relaxed)
            }
            DirectCacheNode::Readguard(rw_lock_read_guard) => {
                rw_lock_read_guard.approx_memory.load(Ordering::Relaxed)
            }
        }
    }

    fn get_eviction_policy(&self) -> Option<std::sync::MutexGuard<'_, crate::db::eviction::strategy::EvictionStrategy>> {
        match self {
            DirectCacheNode::Writeguard(rw_lock_write_guard) => {
                let lock = rw_lock_write_guard.evicition.lock().unwrap();
                Some(lock)
            }
            DirectCacheNode::Readguard(_rw_lock_read_guard) => None,
        }
    }

    fn add_memory(&self, size: usize) {
        match self {
            DirectCacheNode::Writeguard(rw_lock_write_guard) => {
                rw_lock_write_guard
                    .approx_memory
                    .fetch_add(size, Ordering::Relaxed);
                GLOBAL_MEMORY.fetch_add(size, Ordering::Relaxed);
            }
            DirectCacheNode::Readguard(rw_lock_read_guard) => {
                rw_lock_read_guard
                    .approx_memory
                    .fetch_add(size, Ordering::Relaxed);
                GLOBAL_MEMORY.fetch_add(size, Ordering::Relaxed);
            }
        }
    }

    fn sub_memory(&self, size: usize) {
        match self {
            DirectCacheNode::Writeguard(rw_lock_write_guard) => {
                rw_lock_write_guard
                    .approx_memory
                    .fetch_sub(size, Ordering::Relaxed);
                GLOBAL_MEMORY.fetch_sub(size, Ordering::Relaxed);
            }
            DirectCacheNode::Readguard(rw_lock_read_guard) => {
                rw_lock_read_guard
                    .approx_memory
                    .fetch_sub(size, Ordering::Relaxed);
                GLOBAL_MEMORY.fetch_sub(size, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::EvictionType,
        core_time::CACHED_TIME_MS,
        types::{Element, Value},
    };
    use bytes::Bytes;
    use std::sync::atomic::Ordering;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn expired_write_lookup_cleans_memory_and_eviction_metadata() {
        CACHED_TIME_MS.store(100, Ordering::Relaxed);
        let cache = Arc::new(RwLock::new(MemoryCacheNode::new(&EvictionType::LRU)));
        let key = Arc::new("expired".to_string());
        let value = ValueEntry::new(
            Value::Simple(Element::String(Bytes::from_static(b"value"))),
            Some(50),
        );

        let mut node = DirectCacheNode::Writeguard(cache.write_owned().await);
        node.insert(key.clone(), value);
        assert!(node.get_memory_usage() > 0);

        assert!(node.select(&key).is_none());
        assert_eq!(node.get_memory_usage(), 0);
        assert!(
            node.get_eviction_policy()
                .unwrap()
                .get_random_sample_key()
                .is_none()
        );
    }
}
