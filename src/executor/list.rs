use crate::{
    executor::{CommandContext, Executor, parse_int_from_bytes},
    db::LockedDb,
    error::{Frame, KvError, LPopCommand, LPushCommand, BLPopCommand},
    types::{Element, Value, ValueEntry},
};
use crate::db::eviction::traits::KvOperator;
use std::collections::VecDeque;

impl Executor for LPushCommand {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        let mut values_to_push = VecDeque::new();

        if let CommandContext::Normal { db, .. } = &ctx {
            let select_db = crate::context::CONN_STATE.with(|state| state.selected_db);
            let mut queues = db.store.blocking_queues[select_db].lock().await;
            if let Some(waiting_senders) = queues.get_mut(&self.key) {
                for item in &self.values {
                    let mut sent = false;
                    while let Some(tx) = waiting_senders.pop_front() {
                        if tx.send(item.clone()).is_ok() {
                            sent = true;
                            break;
                        }
                    }
                    if !sent {
                        values_to_push.push_back(item.clone());
                    }
                }
                if waiting_senders.is_empty() {
                    queues.remove(&self.key);
                }
            } else {
                values_to_push.extend(self.values.iter().cloned());
            }
        } else {
            values_to_push.extend(self.values.iter().cloned());
        }

        let mut pushed_count = 0;
        let new_len = {
            let mut own_lock;
            let mut sessions_guard;
            let map = get_write_lock!(ctx, &self.key, own_lock, sessions_guard);

            if values_to_push.is_empty() {
                // Return current length if list exists, else 0
                if let Some(entry) = map.select(&self.key) {
                    if let Value::List(deque, _) = &entry.data {
                        return Ok(Frame::Integer(deque.len() as i64));
                    }
                }
                return Ok(Frame::Integer(0));
            }

            let (mut list, expires_at, mut elements_heap) = match map.take(&self.key) {
                Some(entry) => {
                    let exp = entry.expires_at;
                    match entry.data {
                        Value::List(deque, size) => (deque, exp, size),
                        _ => {
                            map.insert(self.key.clone(), entry);
                            return Ok(Frame::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .into(),
                            ));
                        }
                    }
                }
                None => (VecDeque::new(), None, 0),
            };

            for item in values_to_push {
                let el = Element::String(item);
                elements_heap += el.heap_size();
                list.push_front(el);
                pushed_count += 1;
            }

            let new_len = list.len() as i64;
            map.insert(
                self.key.clone(),
                ValueEntry::new(Value::List(list, elements_heap), expires_at),
            );

            new_len
        };
        
        // AOF 
        if pushed_count > 0 {
            ctx.send_aof(&crate::error::Command::LPush(self.clone())).await;
        }

        Ok(Frame::Integer(new_len))
    }
}

impl Executor for LPopCommand {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        let frame = {
            let mut own_lock;
            let mut sessions_guard;
            let map = get_write_lock!(ctx, &self.key, own_lock, sessions_guard);

            let (mut list, expires_at, mut elements_heap) = match map.take(&self.key) {
                Some(entry) => {
                    let exp = entry.expires_at;
                    match entry.data {
                        Value::List(deque, size) => (deque, exp, size),
                        _ => {
                            map.insert(self.key.clone(), entry);
                            return Ok(Frame::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .into(),
                            ));
                        }
                    }
                }
                None => return Ok(Frame::Null),
            };

            if let Some(item) = list.pop_front() {
                elements_heap -= item.heap_size();
                let frame = match item {
                    Element::String(bytes) => Frame::Bulk(bytes),
                    Element::Int(i) => Frame::Bulk(parse_int_from_bytes(i)),
                };

                if !list.is_empty() {
                    map.insert(
                        self.key.clone(),
                        ValueEntry::new(Value::List(list, elements_heap), expires_at),
                    );
                } else {
                    map.delete(&self.key);
                }

                ctx.send_aof(&crate::error::Command::LPop(self.clone()))
                    .await;
                frame
            } else {
                Frame::Null
            }
        };
        Ok(frame)
    }
}

impl Executor for BLPopCommand {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        let mut frame_bytes_opt = None;
        let (db_opt, rx) = {
            let mut own_lock;
            let mut sessions_guard;
            let map = get_write_lock!(ctx, &self.key, own_lock, sessions_guard);
            
            // First check if list has data
            if let Some(entry) = map.take(&self.key) {
                let exp = entry.expires_at;
                match entry.data {
                    Value::List(mut list, mut elements_heap) => {
                        if let Some(item) = list.pop_front() {
                            elements_heap -= item.heap_size();
                            let frame_bytes = match item {
                                Element::String(bytes) => bytes,
                                Element::Int(i) => parse_int_from_bytes(i),
                            };
                            if !list.is_empty() {
                                map.insert(self.key.clone(), ValueEntry::new(Value::List(list, elements_heap), exp));
                            } else {
                                map.delete(&self.key);
                            }
                            frame_bytes_opt = Some(frame_bytes);
                            (None, None)
                        } else {
                            map.insert(self.key.clone(), ValueEntry::new(Value::List(list, elements_heap), exp));
                            (None, None)
                        }
                    },
                    _ => {
                        map.insert(self.key.clone(), entry);
                        return Ok(Frame::Error("WRONGTYPE Operation against a key holding the wrong kind of value".into()));
                    }
                }
            } else {
                // list empty
                let db = match &ctx {
                    CommandContext::Normal { db, .. } => Some(db.clone()),
                    _ => None, // Lua scripts or Recovery shouldn't block!
                };
                
                if let Some(db) = &db {
                    // Register handle
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    let select_db = crate::context::CONN_STATE.with(|state| state.selected_db);
                    let mut queues = db.store.blocking_queues[select_db].lock().await;
                    queues.entry(self.key.clone()).or_insert_with(VecDeque::new).push_back(tx);
                    
                    (Some(db.clone()), Some(rx))
                } else {
                    (None, None)
                }
            }
        }; // Lock is dropped here!

        // If we popped a value, return it
        if let Some(frame_bytes) = frame_bytes_opt {
            ctx.send_aof(&crate::error::Command::LPop(crate::error::LPopCommand { key: self.key.clone() })).await;
            return Ok(Frame::Array(vec![
                Frame::Bulk(bytes::Bytes::copy_from_slice(self.key.as_bytes())), 
                Frame::Bulk(frame_bytes)
            ]));
        }
        
        // Wait on channel if applicable
        if let Some(rx) = rx {
            let wait_duration = if self.timeout == 0 {
                std::time::Duration::from_secs(u64::MAX) // wait forever
            } else {
                std::time::Duration::from_secs(self.timeout)
            };
            
            match tokio::time::timeout(wait_duration, rx).await {
                Ok(Ok(bytes)) => {
                    ctx.send_aof(&crate::error::Command::LPop(crate::error::LPopCommand { key: self.key.clone() })).await;
                    Ok(Frame::Array(vec![
                        Frame::Bulk(bytes::Bytes::copy_from_slice(self.key.as_bytes())), 
                        Frame::Bulk(bytes)
                    ]))
                },
                _ => {
                    Ok(Frame::Null)
                }
            }
        } else {
            Ok(Frame::Null)
        }
    }
}
