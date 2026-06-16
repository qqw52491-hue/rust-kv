use crate::Db;
use crate::aof_exchange::AofContent;
use crate::command_execute::{CommandContext, CommandExecutor};
use crate::context::ConnectionContent;
use crate::db::LockedDb;
use crate::error::{Command, Frame, KvError, LockSpec};

// 假定：Command: Clone
pub async fn execute_command(command: Command, db: &Db) -> Result<Frame, KvError> {
    let ctx = CommandContext {
        db: Some(db.clone()),
        connect_content: None,
        lua_sessions: None,
    };
    command.execute(ctx).await
}

macro_rules! delegate_execute {
    ($self:expr, $ctx:expr, [ $($variant:ident),+ $(,)? ]) => {
        match $self {
            $( Command::$variant(c) => c.execute($ctx).await, )+
        }
    };
}

impl CommandExecutor for Command {
    async fn execute(
        &self,
        ctx: CommandContext,
    ) -> Result<Frame, KvError> {
        delegate_execute!(self, ctx, [
            Get, Set, Ping, Unimplement, EvalCommand,
            LPush, LPop, HSet, HGet, HDel, MSet, MGet
        ])
    }
}

pub async fn execute_command_normal(
    command: Command,
    db: &Db,
    connect_content: ConnectionContent,
) -> Result<Frame, KvError> {
    let ctx = CommandContext {
        db: Some(db.clone()),
        connect_content: Some(connect_content.clone()),
        lua_sessions: None,
    };
    
    let frame: Frame = command.execute(ctx).await?;
    
    //在这里同意执行aof 正常情况下的限定执行
    command
        .exe_aof_command(AofContent {
            aof_tx: &connect_content.aof_tx,
            shutdown_tx: &connect_content.shutdown_tx,
        })
        .await;
        
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
