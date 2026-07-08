use bytes::Bytes;

use crate::{
    command_execute::{
        CommandContext, CommandExecutor, bytes_to_i64_fast, calculate_expiration_timestamp_ms,
        parse_int_from_bytes,
    },
    db::LockedDb,
    error::{Frame, GetCommand, KvError, SetCommand},
    types::{Element, Value, ValueEntry},
};

impl CommandExecutor for SetCommand {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        let time_expire_u64: Option<u64>;
        let time_expire = if let Some(expire) = &self.expiration {
            time_expire_u64 = Some(calculate_expiration_timestamp_ms(expire));
            time_expire_u64
        } else {
            None
        };
        //再这里创建value
        let value_obj = match bytes_to_i64_fast(&self.value) {
            Some(i) => ValueEntry::new(Value::Simple(Element::Int(i)), time_expire),
            None => ValueEntry::new(
                Value::Simple(Element::String(self.value.clone())),
                time_expire,
            ),
        };

        {
            let mut own_lock;
            let mut sessions_guard;
            let map = get_write_lock!(ctx, &self.key, own_lock, sessions_guard);
            map.insert(self.key.clone(), value_obj).await;

            ctx.send_aof(&crate::error::Command::Set(self.clone()))
                .await;
        };

        Ok(Frame::Simple("OK".to_string()))
    }
}

impl CommandExecutor for GetCommand {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        let mut own_lock;
        let mut sessions_guard;
        let map = get_read_lock!(ctx, &self.key, own_lock, sessions_guard);
        let value = map.select(&self.key.clone()).await;
        match value {
            Some(entry) => {
                let data = entry.data.clone();
                //这是处理字符串的方法
                match data {
                    Value::Simple(Element::String(bytes)) => Ok(Frame::Bulk(bytes)),
                    //性能优化
                    Value::Simple(Element::Int(i)) => {
                        let bytes = parse_int_from_bytes(i);
                        Ok(Frame::Bulk(Bytes::from(bytes)))
                    }
                    _ => Ok(Frame::Null), // 如果不是字符串类型，返回 Null
                }
            }
            None => Ok(Frame::Null),
        }
    }
}

use crate::domain::MSetCommand;

impl CommandExecutor for MSetCommand {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        let mut sorted_pairs = self.keys_and_values.clone();
        // 按照 shard_index 以及 key 排序，防止死锁
        // 但为了简单和演示，我们直接按 key 的字典序排
        sorted_pairs.sort_by(|a, b| a.0.cmp(&b.0));

        {
            for (key, val) in &sorted_pairs {
                let mut own_lock;
                let mut sessions_guard;
                let map = get_write_lock!(ctx, key, own_lock, sessions_guard);

                let value_entry = match bytes_to_i64_fast(val) {
                    Some(i) => ValueEntry::new(Value::Simple(Element::Int(i)), None),
                    None => ValueEntry::new(Value::Simple(Element::String(val.clone())), None),
                };

                map.insert(key.clone(), value_entry).await;
            }

            ctx.send_aof(&crate::error::Command::MSet(self.clone()))
                .await;
        };

        Ok(Frame::Simple("OK".to_string()))
    }
}

use crate::domain::MGetCommand;

impl CommandExecutor for MGetCommand {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        let mut results = Vec::new();
        // MGET 不需要提前排序防死锁，因为读锁允许并发读取，
        // 且按给定顺序返回结果是必须的，不需要 sort_by 改变顺序。

        for key in &self.keys {
            let mut own_lock;
            let mut sessions_guard;
            let map = get_read_lock!(ctx, key, own_lock, sessions_guard);
            let value = map.select(key).await;

            let frame = match value {
                Some(entry) => {
                    let data = entry.data.clone();
                    match data {
                        Value::Simple(Element::String(bytes)) => Frame::Bulk(bytes),
                        Value::Simple(Element::Int(i)) => {
                            let bytes = parse_int_from_bytes(i);
                            Frame::Bulk(Bytes::from(bytes))
                        }
                        _ => Frame::Null, // MGET 获取非字符串类型时返回 Null
                    }
                }
                None => Frame::Null,
            };
            results.push(frame);
        }

        Ok(Frame::Array(results))
    }
}
