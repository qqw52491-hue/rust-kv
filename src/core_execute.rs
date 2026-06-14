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
    };
    command.execute(ctx, None).await
}

impl CommandExecutor for Command {
    async fn execute(
        &self,
        ctx: CommandContext,
        db_lock: Option<&mut LockedDb>,
    ) -> Result<Frame, KvError> {
        match self {
            Command::Get(c) => c.execute(ctx, db_lock).await,
            Command::Set(c) => c.execute(ctx, db_lock).await,
            Command::Ping(c) => c.execute(ctx, db_lock).await,
            Command::Unimplement(c) => c.execute(ctx, db_lock).await,
            Command::EvalCommand(c) => c.execute(ctx, db_lock).await,
            Command::LPush(c) => c.execute(ctx, db_lock).await,
            Command::LPop(c) => c.execute(ctx, db_lock).await,
            Command::HSet(c) => c.execute(ctx, db_lock).await,
            Command::HGet(c) => c.execute(ctx, db_lock).await,
            Command::HDel(c) => c.execute(ctx, db_lock).await,
        }
    }
}

pub async fn execute_command_normal(
    command: Command,
    db: &Db,
    connect_content: ConnectionContent,
) -> Result<Frame, KvError> {
    //这里已经是脱离所有权了 开始独立拿出来用了
    let mut lock = get_command_lock(&command, db).await;
    
    let ctx = CommandContext {
        db: Some(db.clone()),
        connect_content: Some(connect_content.clone()),
    };
    
    let frame: Frame = command.execute(ctx, lock.as_mut()).await?;
    
    //在这里同意执行aof 正常情况下的限定执行
    command
        .exe_aof_command(AofContent {
            aof_tx: &connect_content.aof_tx,
            shutdown_tx: &connect_content.shutdown_tx,
        })
        .await;
        
    Ok(frame)
}

pub async fn get_command_lock<'a>(command: &Command, db: &'a Db) -> Option<LockedDb> {
    match command.lock_spec() {
        LockSpec::Write(key) => Some(db.store.lock_write(key).await.into()),
        LockSpec::Read(key) => Some(db.store.lock_read(key).await.into()),
        LockSpec::None => None,
    }
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
