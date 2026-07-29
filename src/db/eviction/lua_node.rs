use std::collections::HashMap;
use std::sync::Arc;

use crate::db::eviction::direct_node::DirectCacheNode;
use crate::db::eviction::traits::{KvOperator, Transactional};
use crate::types::ValueEntry;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EvictionType;
    use crate::db::eviction::cache_store::MemoryCacheNode;
    use crate::domain::Element;
    use crate::types::{Value, ValueEntry};
    use bytes::Bytes;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_lua_atomicity() {
        // 1. 初始化一个真实的底层数据库分片 (MemoryCacheNode)
        let cache_node = Arc::new(RwLock::new(MemoryCacheNode::new(&EvictionType::LRU)));
        let key = Arc::new("my_key".to_string());
        let val = ValueEntry::new(Value::Simple(Element::String(Bytes::from("my_val"))), None);
        // ==========================
        // 场景 A：模拟 Lua 脚本中途报错（回滚）
        // ==========================
        {
            // 拿到真实的读写锁
            let write_guard = cache_node.clone().write_owned().await;
            let direct = DirectCacheNode::Writeguard(write_guard);
            // 包裹上 Lua 的“防脏写手套”
            let mut lua_node = LuaCacheNode::new(direct);

            // Lua 脚本开始疯狂写数据
            lua_node.insert(key.clone(), val.clone());

            // 【核心断言 1】：这时候去查真实底层数据库（db_store），里面一定是空的！
            // 因为数据全被截留在 lua_node 的 differ_map 里了。
            assert!(
                lua_node.db_store.select(&key).is_none(),
                "底层数据被污染了！"
            );

            // 💥 突然，Lua 脚本报错退出了！
            // 我们不调用 `lua_node.commit()`，直接结束这个作用域（触发 Drop 丢弃机制）
        }

        // 【核心断言 2】：我们再次检查真实数据库，数据完全干干净净，没有脏写，实现了完美回滚！
        {
            let read_guard = cache_node.read().await;
            assert!(
                read_guard.db_store.get(&key).is_none(),
                "回滚失败，脏数据写入了真库！"
            );
        }

        // ==========================
        // 场景 B：模拟 Lua 脚本执行成功（提交）
        // ==========================
        {
            let write_guard = cache_node.clone().write_owned().await;
            let direct = DirectCacheNode::Writeguard(write_guard);
            let mut lua_node = LuaCacheNode::new(direct);

            // Lua 脚本正常写数据
            lua_node.insert(key.clone(), val.clone());

            // 脚本跑完了，没有报错，我们主动发出提交指令！
            lua_node.commit();
        }

        // 【核心断言 3】：提交后，真实数据库里终于有数据了！
        {
            let read_guard = cache_node.read().await;
            assert!(
                read_guard.db_store.get(&key).is_some(),
                "提交失败，数据没进入真库！"
            );
        }
    }
}
