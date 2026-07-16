use std::io;
use thiserror::Error;

// ─────────────────────────────────────────────
// 项目统一错误类型
// ─────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum KvError {
    #[error("IO 错误: {0}")]
    Io(#[from] io::Error),

    #[error("协议解析错误: {0}")]
    ProtocolError(String),

    #[error("意外的连接关闭")]
    UnexpectedEof,

    #[error("暂时没有实现")]
    Unimplement,

    #[error("无意义错误")]
    None,
}

// ─────────────────────────────────────────────
// Re-export：让旧的 `use crate::error::xxx` 在迁移期间仍然可用
// 后续可按需删除，统一改为 `use crate::command::xxx`
// ─────────────────────────────────────────────
pub use crate::domain::command::{
    Command, EvalCommand, Expiration, GetCommand, LPopCommand, LPushCommand, BLPopCommand, LockSpec, PingCommand,
    SetCommand, SetCondition, UnimplementCommand, HSetCommand, HGetCommand, HDelCommand, MSetCommand, MGetCommand, MultiCommand, ExecCommand,
};
pub use crate::domain::protocol::{Frame, IsAof, ToBulk};
