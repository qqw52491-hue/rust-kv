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
        _ctx: CommandContext,
        db_lock: Option<&mut LockedDb>,
    ) -> Result<Frame, KvError> {
        if let Some(LockedDb::Write(map)) = db_lock {
            let (mut list, expires_at) = match map.take(&self.key).await {
                Some(entry) => {
                    let exp = entry.expires_at;
                    match entry.data {
                        Value::List(deque) => (deque, exp),
                        _ => {
                            // 还原值，返回类型错误
                            map.insert(self.key.clone(), entry).await;
                            return Ok(Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()));
                        }
                    }
                }
                None => (VecDeque::new(), None),
            };

            for val in &self.values {
                list.push_front(Element::String(val.clone()));
            }

            let new_len = list.len() as i64;
            map.insert(self.key.clone(), ValueEntry::new(Value::List(list), expires_at)).await;
            Ok(Frame::Integer(new_len))
        } else {
            Err(KvError::ProtocolError("LPUSH requires a write lock".into()))
        }
    }
}

impl CommandExecutor for LPopCommand {
    async fn execute(
        &self,
        _ctx: CommandContext,
        db_lock: Option<&mut LockedDb>,
    ) -> Result<Frame, KvError> {
        if let Some(LockedDb::Write(map)) = db_lock {
            let (mut list, expires_at) = match map.take(&self.key).await {
                Some(entry) => {
                    let exp = entry.expires_at;
                    match entry.data {
                        Value::List(deque) => (deque, exp),
                        _ => {
                            // 还原值，返回类型错误
                            map.insert(self.key.clone(), entry).await;
                            return Ok(Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()));
                        }
                    }
                }
                None => return Ok(Frame::Null),
            };

            match list.pop_front() {
                Some(element) => {
                    let resp = match element {
                        Element::String(bytes) => Frame::Bulk(bytes),
                        Element::Int(i) => Frame::Bulk(parse_int_from_bytes(i)),
                    };
                    if !list.is_empty() {
                        map.insert(self.key.clone(), ValueEntry::new(Value::List(list), expires_at)).await;
                    }
                    Ok(resp)
                }
                None => Ok(Frame::Null),
            }
        } else {
            Err(KvError::ProtocolError("LPOP requires a write lock".into()))
        }
    }
}
