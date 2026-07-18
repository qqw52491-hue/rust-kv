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
                //首先标记出触发淘汰策略
                rw_lock_write_guard
                    .evicition
                    .lock()
                    .unwrap()
                    .on_write(key.clone());
                let size_before = match rw_lock_write_guard.db_store.get(&key) {
                    Some(entry) => entry.data_size,
                    None => 0,
                };

                //值差异
                let memory_differ = value.data_size as isize - size_before as isize;

                //插入数值的时候 消耗掉这个
                rw_lock_write_guard.db_store.insert(key, value);

                // 2. 根据差值的正负，决定是加还是减
                if memory_differ > 0 {
                    // 内存增加了：转成 usize 加进去
                    rw_lock_write_guard
                        .approx_memory
                        .fetch_add(memory_differ as usize, Ordering::Relaxed);
                    GLOBAL_MEMORY.fetch_add(memory_differ as usize, Ordering::Relaxed);
                } else if memory_differ < 0 {
                    // 内存减少了：取绝对值（变成正数），然后减出去
                    // (-memory_differ) 就变成了正数，比如 -50 变成 50
                    rw_lock_write_guard
                        .approx_memory
                        .fetch_sub((-memory_differ) as usize, Ordering::Relaxed);
                    GLOBAL_MEMORY.fetch_sub((-memory_differ) as usize, Ordering::Relaxed);
                }
            }
            DirectCacheNode::Readguard(_rw_lock_read_guard) => {}
        }
    }

    fn delete(&mut self, key: &Arc<String>) {
        match self {
            DirectCacheNode::Writeguard(rw_lock_write_guard) => {
                if let Some(value) = rw_lock_write_guard.db_store.remove(key) {
                    //触发淘汰策略
                    rw_lock_write_guard
                        .evicition
                        .lock()
                        .unwrap()
                        .on_delete(key.clone());
                    rw_lock_write_guard
                        .approx_memory
                        .fetch_sub(value.data_size, Ordering::Relaxed);
                    GLOBAL_MEMORY.fetch_sub(value.data_size, Ordering::Relaxed);
                }
            }
            DirectCacheNode::Readguard(_rw_lock_read_guard) => {}
        }
    }

    fn take(&mut self, key: &Arc<String>) -> Option<ValueEntry> {
        match self {
            DirectCacheNode::Writeguard(rw_lock_write_guard) => {
                if let Some(value) = rw_lock_write_guard.db_store.remove(key) {
                    //触发淘汰策略
                    rw_lock_write_guard
                        .evicition
                        .lock()
                        .unwrap()
                        .on_delete(key.clone());
                    rw_lock_write_guard
                        .approx_memory
                        .fetch_sub(value.data_size, Ordering::Relaxed);
                    GLOBAL_MEMORY.fetch_sub(value.data_size, Ordering::Relaxed);
                    Some(value)
                } else {
                    None
                }
            }
            DirectCacheNode::Readguard(_rw_lock_read_guard) => None,
        }
    }

    /*
    读写在内核代理层就完成
     */
    fn select(&mut self, key: &Arc<String>) -> Option<&ValueEntry> {
        match self {
            // 1. 【语法修正】这里不要写 ref mut，直接写变量名 guard
            // 因为 self 是 &mut，guard 自动就是可变引用
            // guard 的类型其实是： &mut OwnedRwLockWriteGuard<MemoryCacheNode>
            DirectCacheNode::Writeguard(guard) => {
                // 这里需要“剥两层壳”：
                // 1. 第一层 * 解开 &mut 引用，拿到 OwnedRwLockWriteGuard；
                // 2. 第二层 * 通过 Guard 的 DerefMut，拿到底层 MemoryCacheNode；
                // 3. 最外层 &mut 再取得 Node 的可变引用。
                //
                // 显式拿到 node 后，编译器可以清楚地区分 db_store、evicition
                // 和内存计数器这几个互不重叠的字段，后面才能安全地分别操作它们。
                let node = &mut **guard;

                // 第一查只计算过期标记，不把 db_store 中 ValueEntry 的引用带到后面。
                // 这样临时的不可变借用会在 match 结束时释放，随后才能执行 remove。
                let is_expired = match node.db_store.get(key) {
                    Some(value) => value
                        .expires_at
                        .is_some_and(|expires_at| get_cached_time_ms() > expires_at),
                    None => return None,
                };

                if is_expired {
                    // 第二查执行真正删除。这里不能只写 db_store.remove：
                    // 数据存储、淘汰策略和两级内存计数是一组必须同步维护的不变量。
                    // 少更新其中任何一个，都会留下幽灵 LRU 节点或错误的内存占用。
                    if let Some(value) = node.db_store.remove(key) {
                        node.evicition.lock().unwrap().on_delete(key.clone());
                        node.approx_memory
                            .fetch_sub(value.data_size, Ordering::Relaxed);
                        GLOBAL_MEMORY.fetch_sub(value.data_size, Ordering::Relaxed);
                    }
                    return None;
                }

                // 只有 key 确实存在且未过期，才把它标记为最近访问。
                node.evicition.lock().unwrap().on_read(key);
                node.db_store.get(key)
            }
            DirectCacheNode::Readguard(rw_lock_read_guard) => {
                // 读锁可以判断过期，但不能修改 db_store 做物理删除。
                // 因此读路径只向业务层返回 None；之后由写路径或后台 TTL 任务完成清理。
                let node = &**rw_lock_read_guard;
                let value = node.db_store.get(key)?;

                if value
                    .expires_at
                    .is_some_and(|expires_at| get_cached_time_ms() > expires_at)
                {
                    return None;
                }

                // 只有真实且未过期的命中才更新淘汰策略。
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

    fn get_eviction_policy(&self) -> Option<std::sync::MutexGuard<'_, Box<dyn EvictionPolicy>>> {
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
