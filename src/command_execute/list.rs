use std::collections::VecDeque;
use crate::{
    command_execute::{CommandContext, CommandExecutor, parse_int_from_bytes},
    db::LockedDb,
    error::{Frame, KvError, LPushCommand, LPopCommand},
    types::{Element, Value, ValueEntry},
};

impl CommandExecutor for LPushCommand {
    async fn execute(
        &self,
        ctx: CommandContext,
    ) -> Result<Frame, KvError> {
        let mut own_lock;
        let mut sessions_guard;
        let map = get_write_lock!(ctx, &self.key, own_lock, sessions_guard);

        let (mut list, expires_at, mut elements_heap) = match map.take(&self.key).await {
            Some(entry) => {
                let exp = entry.expires_at;
                match entry.data {
                    Value::List(deque, size) => (deque, exp, size),
                    _ => {
                        // 还原值，返回类型错误
                        map.insert(self.key.clone(), entry).await;
                        return Ok(Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()));
                    }
                }
            }
            None => (VecDeque::new(), None, 0),
        };

        for item in &self.values {
            let el = Element::String(item.clone());
            elements_heap += el.heap_size();
            list.push_front(el);
        }

        let new_len = list.len() as i64;
        map.insert(self.key.clone(), ValueEntry::new(Value::List(list, elements_heap), expires_at)).await;
        Ok(Frame::Integer(new_len))
    }
}

impl CommandExecutor for LPopCommand {
    async fn execute(
        &self,
        ctx: CommandContext,
    ) -> Result<Frame, KvError> {
        let mut own_lock;
        let mut sessions_guard;
        let map = get_write_lock!(ctx, &self.key, own_lock, sessions_guard);

        let (mut list, expires_at, mut elements_heap) = match map.take(&self.key).await {
            Some(entry) => {
                let exp = entry.expires_at;
                match entry.data {
                    Value::List(deque, size) => (deque, exp, size),
                    _ => {
                        // 如果不是 List 类型，把原值塞回去并返回错误
                        map.insert(self.key.clone(), entry).await;
                        return Ok(Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()));
                    }
                }
            }
            None => return Ok(Frame::Null), // 键不存在，返回 Null
        };

        if let Some(item) = list.pop_front() {
            elements_heap -= item.heap_size();
            let frame = match item {
                Element::String(bytes) => Frame::Bulk(bytes),
                Element::Int(i) => Frame::Bulk(parse_int_from_bytes(i)),
            };
            
            // 只有列表非空才放回 map
            if !list.is_empty() {
                map.insert(self.key.clone(), ValueEntry::new(Value::List(list, elements_heap), expires_at)).await;
            } else {
                map.delete(&self.key).await;
            }
            Ok(frame)
        } else {
            Ok(Frame::Null)
        }
    }
}
