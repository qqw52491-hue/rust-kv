use crate::types::ValueEntry;
use std::sync::Arc;

pub trait EvictionPolicy: Send + Sync {
    // 当写入时，策略需要做什么？
    fn on_write(&mut self, key: Arc<String>);
    // 当读取时，策略需要做什么？
    fn on_read(&mut self, key: &Arc<String>);
    // 当删除时...
    fn on_delete(&mut self, key: Arc<String>);
    // “获取里面的 key 数组” -> 抽象成 -> “给我一个随机 key”
    fn get_random_sample_key(&self) -> Option<Arc<String>>;
    // 挑选一个删除者
    fn pop_victim(&mut self) -> Option<Arc<String>>;
}

// 这是一个纯净的接口，只管数据读写
pub trait KvOperator: Send + Sync {
    fn insert(&mut self, key: Arc<String>, value: ValueEntry);
    fn select(&mut self, key: &Arc<String>) -> Option<&ValueEntry>;
    fn take(&mut self, key: &Arc<String>) -> Option<ValueEntry>;
    fn delete(&mut self, key: &Arc<String>);
}

pub trait Transactional: KvOperator {
    fn commit(&mut self);
}

//数据库最基本的三个操作
pub trait LockOwner: KvOperator {
    // 1. 暴露内存大小 (AtomicUsize 通常只返回数值 usize)
    fn get_memory_usage(&self) -> usize;

    // 2. 暴露驱逐策略 (使用枚举，避免动态分发开销)
    fn get_eviction_policy(&self) -> Option<std::sync::MutexGuard<'_, crate::db::eviction::strategy::EvictionStrategy>>;

    // 修改内存记账 (封装成行为更好，不要直接暴露 Atomic)
    fn add_memory(&self, size: usize);
    fn sub_memory(&self, size: usize);
}
