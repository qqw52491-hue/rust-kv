use crate::{
    executor::{CommandContext, Executor},
    db::LockedDb,
    db::eviction::traits::KvOperator,
    error::{Frame, KvError},
    domain::command::{ZAddCommand, ZScoreCommand, ZRankCommand, ZRangeCommand, ZRemCommand},
    types::{Value, ValueEntry},
    db::zset::ZSet,
};
use bytes::Bytes;

impl Executor for ZAddCommand {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        let mut own_lock;
        let mut sessions_guard;
        let mut map = get_write_lock!(ctx, &self.key, own_lock, sessions_guard);

        let mut added = 0;
        let entry = map.take(&self.key);
        match entry {
            Some(val) => {
                let expires_at = val.expires_at;
                if let Value::ZSet(mut zset, _) = val.data {
                    if zset.insert(self.score, self.member.clone()) {
                        added = 1;
                    }
                    let new_size = zset.heap_memory_size();
                    map.insert(self.key.clone(), ValueEntry::new(Value::ZSet(zset, new_size), expires_at));
                } else {
                    map.insert(self.key.clone(), val);
                    return Err(KvError::ProtocolError("WRONGTYPE Operation against a key holding the wrong kind of value".into()));
                }
            }
            None => {
                let mut zset = ZSet::new();
                zset.insert(self.score, self.member.clone());
                added = 1;
                let size = zset.heap_memory_size();
                map.insert(self.key.clone(), ValueEntry::new(Value::ZSet(zset, size), None));
            }
        }
        ctx.send_aof(&crate::error::Command::ZAdd(self.clone())).await;
        Ok(Frame::Integer(added))
    }
}

impl Executor for ZScoreCommand {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        let mut own_lock;
        let mut sessions_guard;
        let map = get_read_lock!(ctx, &self.key, own_lock, sessions_guard);

        if let Some(val) = map.select(&self.key) {
            if let Value::ZSet(zset, _) = &val.data {
                if let Some(score) = zset.score(&self.member) {
                    return Ok(Frame::Bulk(Bytes::from(score.to_string())));
                }
            } else {
                return Err(KvError::ProtocolError("WRONGTYPE Operation against a key holding the wrong kind of value".into()));
            }
        }
        Ok(Frame::Null)
    }
}

impl Executor for ZRankCommand {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        let mut own_lock;
        let mut sessions_guard;
        let map = get_read_lock!(ctx, &self.key, own_lock, sessions_guard);

        if let Some(val) = map.select(&self.key) {
            if let Value::ZSet(zset, _) = &val.data {
                if let Some(rank) = zset.rank(&self.member) {
                    return Ok(Frame::Integer(rank as i64));
                }
            } else {
                return Err(KvError::ProtocolError("WRONGTYPE Operation against a key holding the wrong kind of value".into()));
            }
        }
        Ok(Frame::Null)
    }
}

impl Executor for ZRangeCommand {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        let mut own_lock;
        let mut sessions_guard;
        let map = get_read_lock!(ctx, &self.key, own_lock, sessions_guard);

        if let Some(val) = map.select(&self.key) {
            if let Value::ZSet(zset, _) = &val.data {
                let range = zset.range(self.start, self.stop);
                let mut frames = Vec::with_capacity(range.len());
                for (member, _) in range {
                    frames.push(Frame::Bulk(member));
                }
                return Ok(Frame::Array(frames));
            } else {
                return Err(KvError::ProtocolError("WRONGTYPE Operation against a key holding the wrong kind of value".into()));
            }
        }
        Ok(Frame::Array(Vec::new()))
    }
}

impl Executor for ZRemCommand {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        let mut own_lock;
        let mut sessions_guard;
        let mut map = get_write_lock!(ctx, &self.key, own_lock, sessions_guard);

        let mut removed = 0;
        let entry = map.take(&self.key);
        match entry {
            Some(val) => {
                let expires_at = val.expires_at;
                if let Value::ZSet(mut zset, _) = val.data {
                    if zset.delete(&self.member) {
                        removed = 1;
                        if zset.len() == 0 {
                            // If empty, don't put it back
                        } else {
                            let new_size = zset.heap_memory_size();
                            map.insert(self.key.clone(), ValueEntry::new(Value::ZSet(zset, new_size), expires_at));
                        }
                    } else {
                        let new_size = zset.heap_memory_size();
                        map.insert(self.key.clone(), ValueEntry::new(Value::ZSet(zset, new_size), expires_at));
                    }
                } else {
                    map.insert(self.key.clone(), val);
                    return Err(KvError::ProtocolError("WRONGTYPE Operation against a key holding the wrong kind of value".into()));
                }
            }
            None => {}
        }
        
        if removed > 0 {
            ctx.send_aof(&crate::error::Command::ZRem(self.clone())).await;
        }
        Ok(Frame::Integer(removed))
    }
}
