use bytes::Bytes;
use itoa::Buffer;

use crate::{
    context::ConnectionContent, core_time::get_cached_time_ms, db::{Db, LockedDb}, error::{Frame, KvError}
};
use std::sync::Arc;

#[macro_export]
macro_rules! get_write_lock {
    ($ctx:expr, $key:expr, $own_lock:ident, $sessions_guard:ident) => {
        match &$ctx {
            crate::executor::CommandContext::Lua { lua_sessions } => {
                let shard_index = crate::db::eviction::MemoryCache::get_shard_index($key);
                $sessions_guard = lua_sessions.lock().await;
                $sessions_guard.get_mut(&shard_index).unwrap()
            }
            crate::executor::CommandContext::Normal { db, .. } | crate::executor::CommandContext::Recovery { db } => {
                $own_lock = db.store.lock_write($key).await;
                &mut $own_lock
            }
        }
    };
}

#[macro_export]
macro_rules! get_read_lock {
    ($ctx:expr, $key:expr, $own_lock:ident, $sessions_guard:ident) => {
        match &$ctx {
            crate::executor::CommandContext::Lua { lua_sessions } => {
                let shard_index = crate::db::eviction::MemoryCache::get_shard_index($key);
                $sessions_guard = lua_sessions.lock().await;
                $sessions_guard.get_mut(&shard_index).unwrap()
            }
            crate::executor::CommandContext::Normal { db, .. } | crate::executor::CommandContext::Recovery { db } => {
                $own_lock = db.store.lock_read($key).await;
                &mut $own_lock
            }
        }
    };
}

 mod common;
 mod string;
 mod list;
mod hash;

#[derive(Clone)]
pub enum CommandContext {
    Normal {
        db: Db,
        connect_content: ConnectionContent,
    },
    Lua {
        lua_sessions: Arc<tokio::sync::Mutex<std::collections::HashMap<usize, LockedDb>>>,
    },
    Recovery {
        db: Db,
    },
}

impl CommandContext {
    /// 触发 AOF 日志发送。调用方需自行确保在写锁的作用域 `{ ... }` 内调用该方法。
    pub async fn send_aof(&self, cmd: &crate::error::Command) {
        if let CommandContext::Normal { connect_content, .. } = self {
            if let Err(e) = cmd
                .encode_aof_command(crate::aof_encoder::AofContent {
                    aof_tx: &connect_content.aof_tx,
                    shutdown_tx: &connect_content.shutdown_tx,
                })
                .await
            {
                eprintln!("AOF Append Failed: {}", e);
            }
        }
    }
}

pub trait Executor {
    fn execute(
        &self,
        ctx: CommandContext,
    ) -> impl std::future::Future<Output = Result<Frame, KvError>> + Send ;
}

pub fn calculate_expiration_timestamp_ms(expiration: &crate::error::Expiration) -> u64 {
    let now = get_cached_time_ms();
    match expiration {
        crate::error::Expiration::PX(ms) => now + ms,
        crate::error::Expiration::EX(s) => now + s * 1000,
        crate::error::Expiration::EXAT(s) => *s,
        crate::error::Expiration::PXAT(ms) => *ms,
    }
}

pub fn parse_int_from_bytes(i: i64) -> Bytes {
    let mut buffer = Buffer::new();
    let printed_str = buffer.format(i);
    Bytes::copy_from_slice(printed_str.as_bytes())
}

pub fn bytes_to_i64_fast(b: &Bytes) -> Option<i64> {
    let result = lexical_core::parse::<i64>(b);
    result.ok()
}
