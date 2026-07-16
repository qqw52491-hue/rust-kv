use std::collections::HashMap;
use crate::{
    executor::{CommandContext, Executor, parse_int_from_bytes},
    db::LockedDb,
    db::eviction::traits::KvOperator,
    error::{Frame, KvError, HSetCommand, HGetCommand, HDelCommand},
    types::{Element, Value, ValueEntry},
};

impl Executor for HSetCommand {
    async fn execute(
        &self,
        ctx: CommandContext,
    ) -> Result<Frame, KvError> {
        let added_count = {
            let mut own_lock;
            let mut sessions_guard;
            let map = get_write_lock!(ctx, &self.key, own_lock, sessions_guard);

            let (mut hash_map, expires_at, mut elements_heap) = match map.take(&self.key) {
                Some(entry) => {
                    let exp = entry.expires_at;
                    match entry.data {
                        Value::Hash(h, size) => (h, exp, size),
                        _ => {
                            // 还原值，返回类型错误
                            map.insert(self.key.clone(), entry);
                            return Ok(Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()));
                        }
                    }
                }
                None => (HashMap::new(), None, 0),
            };

            let mut added_count = 0;
            for (field, value) in &self.field_values {
                let el = Element::String(value.clone());
                let added_size = field.len() + el.heap_size();
                
                if let Some(old_val) = hash_map.insert(field.clone(), el) {
                    elements_heap = elements_heap - old_val.heap_size() + value.len();
                } else {
                    elements_heap += added_size;
                    added_count += 1;
                }
            }

            map.insert(self.key.clone(), ValueEntry::new(Value::Hash(hash_map, elements_heap), expires_at));
            
            // 就在锁将要释放前发送 AOF
            ctx.send_aof(&crate::error::Command::HSet(self.clone())).await;
            
            added_count
        }; // <--- 关键点：map 等所有锁在这里安全、清晰地释放

        Ok(Frame::Integer(added_count))
    }
}

impl Executor for HGetCommand {
    async fn execute(
        &self,
        ctx: CommandContext,
    ) -> Result<Frame, KvError> {
        let mut own_lock;
        let mut sessions_guard;
        let map = get_read_lock!(ctx, &self.key, own_lock, sessions_guard);

        match map.select(&self.key) {
            Some(entry) => {
                match &entry.data {
                    Value::Hash(hash_map, _) => {
                        match hash_map.get(&self.field) {
                            Some(element) => {
                                match element {
                                    Element::String(bytes) => Ok(Frame::Bulk(bytes.clone())),
                                    Element::Int(i) => Ok(Frame::Bulk(parse_int_from_bytes(*i))),
                                }
                            }
                            None => Ok(Frame::Null),
                        }
                    }
                    _ => Ok(Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into())),
                }
            }
            None => Ok(Frame::Null),
        }
    }
}

impl Executor for HDelCommand {
    async fn execute(
        &self,
        ctx: CommandContext,
    ) -> Result<Frame, KvError> {
        let removed_count = {
            let mut own_lock;
            let mut sessions_guard;
            let map = get_write_lock!(ctx, &self.key, own_lock, sessions_guard);

            let (mut hash_map, expires_at, mut elements_heap) = match map.take(&self.key) {
                Some(entry) => {
                    let exp = entry.expires_at;
                    match entry.data {
                        Value::Hash(h, size) => (h, exp, size),
                        _ => {
                            map.insert(self.key.clone(), entry);
                            return Ok(Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()));
                        }
                    }
                }
                None => return Ok(Frame::Integer(0)),
            };

            let mut removed_count = 0;
            for field in &self.fields {
                if let Some(old_val) = hash_map.remove(field) {
                    elements_heap -= field.len() + old_val.heap_size();
                    removed_count += 1;
                }
            }

            if !hash_map.is_empty() {
                map.insert(self.key.clone(), ValueEntry::new(Value::Hash(hash_map, elements_heap), expires_at));
            } else {
                map.delete(&self.key);
            }

            ctx.send_aof(&crate::error::Command::HDel(self.clone())).await;
            removed_count
        };

        Ok(Frame::Integer(removed_count))
    }
}
