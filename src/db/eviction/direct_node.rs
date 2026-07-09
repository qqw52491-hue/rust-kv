use std::sync::{Arc, atomic::Ordering};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard};

use crate::types::ValueEntry;
use crate::core_time::get_cached_time_ms;
use crate::db::eviction::traits::{KvOperator, LockOwner, EvictionPolicy};
use crate::db::eviction::cache_store::{MemoryCacheNode, GLOBAL_MEMORY};

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
                // let eviction = guard.evicition.lock().await;
                // 1. 【关键修正】剥两层壳！
                // 第一个 * ：解开 &mut 引用，拿到 Guard 智能指针
                // 第二个 * ：触发 Guard 的 Deref，拿到内部的 MemoryCacheNode
                // &mut    ：重新获取 Node 的可变引用
                let node = &mut **guard;

                // 2. 【简单粗暴】手动拿字段
                // 这样写，编译器 100% 知道 store 和 eviction 是分开的
                // 绝对不会报 "borrowed more than once"
                let store = &mut node.db_store;
                let eviction = &mut node.evicition;

                // 3. 先更新 LRU (操作 eviction)
                eviction.lock().unwrap().on_read(key);

                // 4. 【第一查】只拿 bool 标记
                // 这一步只借用 store 一瞬间，用完立刻释放
                let should_remove = if let Some(v) = store.get(key) {
                    if let Some(t) = v.expires_at {
                        get_cached_time_ms() > t
                    } else {
                        false
                    }
                } else {
                    return None;
                };

                // 5. 【第二查】根据标记行动
                // 此时 store 是完全自由的
                if should_remove {
                    store.remove(key);
                    None
                } else {
                    // 没过期，重新获取并返回
                    store.get(key)
                }
            }
            DirectCacheNode::Readguard(rw_lock_read_guard) => {
                // 1. 拿引用 (0 开销)
                let node = &**rw_lock_read_guard;
                let store = &node.db_store;
                let eviction = &node.evicition; // 这里是 Mutex<Box<dyn Policy>>

                // 2. 更新 LRU (内部可变性，微小开销)
                // 这里的 Mutex 是 std::sync::Mutex，非常快
                eviction.lock().unwrap().on_read(key);
                // 3. 查数据
                if let Some(value) = store.get(key) {
                    // 4. 检查过期
                    if let Some(expire_time) = value.expires_at {
                        if get_cached_time_ms() > expire_time {
                            // 【惰性删除策略】
                            // 发现过期 -> 既然只读锁删不掉 -> 直接返回 None
                            // 此时在业务层看来，key 已经不存在了
                            return None;
                        }
                    }

                    // 5. 命中返回
                    // 记得返回 Clone 的值 (ValueEntry)，不要返回引用
                    return Some(value);
                }

                None
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
