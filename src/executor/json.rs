use crate::{
    executor::{CommandContext, Executor},
    db::LockedDb,
    db::eviction::traits::KvOperator,
    error::{Frame, KvError},
    domain::command::{JsonSetCommand, JsonGetCommand},
    types::{Value, ValueEntry},
};
use bytes::Bytes;
use std::sync::Arc;
use serde_json::Value as JsonValue;

impl Executor for JsonSetCommand {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        // Parse the provided JSON string
        let new_json_val: JsonValue = match serde_json::from_str(&self.value) {
            Ok(v) => v,
            Err(_) => return Err(KvError::ProtocolError("Invalid JSON value".into())),
        };

        {
            let mut own_lock;
            let mut sessions_guard;
            let map = get_write_lock!(ctx, &self.key, own_lock, sessions_guard);

            // Get existing or create new JSON root
            let entry = map.take(&self.key);
            match entry {
                Some(val) => {
                    if let Value::Json(mut root, _) = val.data {
                        if self.path == "/" || self.path == "" {
                            root = new_json_val;
                        } else {
                            if let Some(target) = root.pointer_mut(&self.path) {
                                *target = new_json_val;
                            } else {
                                // Put back the old value
                                map.insert(self.key.clone(), ValueEntry { data: Value::Json(root, 0), ..val });
                                return Err(KvError::ProtocolError("Path does not exist".into()));
                            }
                        }
                        // Update size approximation
                        let new_size = serde_json::to_string(&root).unwrap_or_default().len();
                        map.insert(self.key.clone(), ValueEntry { data: Value::Json(root, new_size), ..val });
                    } else {
                        // Put back
                        map.insert(self.key.clone(), val);
                        return Err(KvError::ProtocolError("WRONGTYPE Operation against a key holding the wrong kind of value".into()));
                    }
                }
                None => {
                    // Create new JSON object if path is root
                    if self.path == "/" || self.path == "" {
                        let size = serde_json::to_string(&new_json_val).unwrap_or_default().len();
                        let new_entry = ValueEntry::new(Value::Json(new_json_val, size), None);
                        map.insert(self.key.clone(), new_entry);
                    } else {
                        return Err(KvError::ProtocolError("could not set path in non-existent key".into()));
                    }
                }
            }

            ctx.send_aof(&crate::error::Command::JsonSet(self.clone())).await;
        }

        Ok(Frame::Simple("OK".to_string()))
    }
}

impl Executor for JsonGetCommand {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        let mut own_lock;
        let mut sessions_guard;
        let map = get_read_lock!(ctx, &self.key, own_lock, sessions_guard);

        let entry = map.select(&self.key);
        match entry {
            Some(val) => {
                if let Value::Json(root, _) = &val.data {
                    if self.path == "/" || self.path == "" {
                        let json_str = serde_json::to_string(root)
                            .unwrap_or_else(|_| "null".to_string());
                        Ok(Frame::Bulk(Bytes::from(json_str)))
                    } else {
                        if let Some(target) = root.pointer(&self.path) {
                            let json_str = serde_json::to_string(target)
                                .unwrap_or_else(|_| "null".to_string());
                            Ok(Frame::Bulk(Bytes::from(json_str)))
                        } else {
                            Ok(Frame::Null)
                        }
                    }
                } else {
                    Err(KvError::ProtocolError("WRONGTYPE Operation against a key holding the wrong kind of value".into()))
                }
            }
            None => Ok(Frame::Null),
        }
    }
}
