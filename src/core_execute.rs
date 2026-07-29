use crate::Db;
use crate::aof_encoder::AofContent;
use crate::context::ConnectionContent;
use crate::db::LockedDb;
use crate::error::{Command, Frame, KvError, LockSpec};
use crate::executor::{CommandContext, Executor};

// 假定：Command: Clone
pub async fn execute_command(command: Command, db: &Db) -> Result<Frame, KvError> {
    let ctx = CommandContext::Recovery { db: db.clone() };
    command.execute(ctx).await
}

macro_rules! delegate_execute {
    ($self:expr, $ctx:expr, [ $($variant:ident),+ $(,)? ]) => {
        match $self {
            $( Command::$variant(c) => c.execute($ctx).await, )+
        }
    };
}

impl Executor for Command {
    async fn execute(&self, ctx: CommandContext) -> Result<Frame, KvError> {
        // 全局 QPS 埋点：每处理一个命令，计数器 +1
        metrics::counter!("kv_commands_total").increment(1);

        delegate_execute!(
            self,
            ctx,
            [
                Get,
                Set,
                Ping,
                Unimplement,
                EvalCommand,
                LPush,
                LPop,
                BLPop,
                HSet,
                HGet,
                HDel,
                MSet,
                MGet,
                Multi,
                Exec,
                MultiGroup,
                JsonSet,
                JsonGet,
                ZAdd,
                ZScore,
                ZRank,
                ZRange,
                ZRem
            ]
        )
    }
}

impl Executor for Vec<Command> {
    async fn execute(&self, _ctx: CommandContext) -> Result<Frame, KvError> {
        Ok(Frame::Null)
    }
}

pub async fn execute_command_normal(
    command: Command,
    db: &Db,
    connect_content: ConnectionContent,
) -> Result<Frame, KvError> {
    let ctx = CommandContext::Normal {
        db: db.clone(),
        connect_content: connect_content.clone(),
    };

    let frame: Frame = command.execute(ctx).await?;

    Ok(frame)
}

impl Frame {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Frame::Simple(s) => format!("+{}\r\n", s).into_bytes(),
            Frame::Error(s) => format!("-{}\r\n", s).into_bytes(),
            Frame::Integer(i) => format!(":{}\r\n", i).into_bytes(),
            Frame::Null => b"$-1\r\n".to_vec(),
            Frame::Bulk(bytes) => {
                let mut buf = format!("${}\r\n", bytes.len()).into_bytes();
                buf.extend_from_slice(bytes);
                buf.extend_from_slice(b"\r\n");
                buf
            }
            Frame::Array(frames) => {
                let mut buf = format!("*{}\r\n", frames.len()).into_bytes();
                for frame in frames {
                    buf.extend_from_slice(&frame.serialize());
                }
                buf
            }
        }
    }
}
