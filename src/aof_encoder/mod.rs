use bytes::Bytes;

use itoa::Buffer;
use tokio::sync::mpsc::Sender;

use crate::{core_time::get_cached_time_ms, error::Command};

mod hash;
mod json;
mod list;
mod string;
mod zset;

pub trait AofEncoder {
    // encode_aof 方法現在接收 AofContent 作為參數！
    async fn encode_aof<'a>(
        &self,
        // 2. 将这个生命周期 'ctx 应用到 CommandContext 的引用上
        ctx: AofContent<'a>,
    ) -> Result<(), String>;
}

/*
基于这个command 指令 实现对应方法
模块是分开的 并不一定就是代表数据结构是分开的 都是针对command 这个命令的
所以一个模块是功能性划分 结构是实体划分 承载结构
*/
impl Command {
    pub async fn encode_aof_command<'a>(&self, ctx: AofContent<'a>) -> Result<(), String> {
        match self {
            Command::Set(set_command) => set_command.encode_aof(ctx).await,
            Command::LPush(lpush_command) => lpush_command.encode_aof(ctx).await,
            Command::LPop(lpop_command) => lpop_command.encode_aof(ctx).await,
            Command::BLPop(_) => Ok(()),
            Command::HSet(hset_command) => hset_command.encode_aof(ctx).await,
            Command::HDel(hdel_command) => hdel_command.encode_aof(ctx).await,
            Command::JsonSet(c) => c.encode_aof(ctx).await,
            Command::ZAdd(c) => c.encode_aof(ctx).await,
            Command::ZRem(c) => c.encode_aof(ctx).await,
            Command::Get(_)
            | Command::HGet(_)
            | Command::JsonGet(_)
            | Command::ZScore(_)
            | Command::ZRank(_)
            | Command::ZRange(_)
            | Command::Ping(_)
            | Command::Unimplement(_)
            | Command::MGet(_)
            | Command::EvalCommand(_) => Ok(()),
            Command::Multi(c) => c.encode_aof(ctx).await,
            Command::Exec(c) => c.encode_aof(ctx).await,
            Command::MultiGroup(cmds) => {
                let mut buf = Frame::Array(vec![Frame::Bulk(Bytes::from("MULTI"))]).serialize();
                let (dummy_tx, mut dummy_rx) = tokio::sync::mpsc::channel(100);
                let (dummy_shutdown_tx, _) = tokio::sync::broadcast::channel(1);
                for cmd in cmds {
                    let dummy_ctx = AofContent {
                        aof_tx: &dummy_tx,
                        shutdown_tx: &dummy_shutdown_tx,
                    };
                    let _ = match cmd {
                        Command::Set(c) => c.encode_aof(dummy_ctx).await,
                        Command::LPush(c) => c.encode_aof(dummy_ctx).await,
                        Command::LPop(c) => c.encode_aof(dummy_ctx).await,
                        Command::HSet(c) => c.encode_aof(dummy_ctx).await,
                        Command::HDel(c) => c.encode_aof(dummy_ctx).await,
                        Command::MSet(c) => c.encode_aof(dummy_ctx).await,
                        Command::JsonSet(c) => c.encode_aof(dummy_ctx).await,
                        Command::ZAdd(c) => c.encode_aof(dummy_ctx).await,
                        Command::ZRem(c) => c.encode_aof(dummy_ctx).await,
                        _ => Ok(()),
                    };
                    while let Ok(msg) = dummy_rx.try_recv() {
                        buf.extend(msg);
                    }
                }
                let exec_buf = Frame::Array(vec![Frame::Bulk(Bytes::from("EXEC"))]).serialize();
                buf.extend(exec_buf);
                ctx.aof_tx.send(buf).await.map_err(|e| e.to_string())
            }
            Command::MSet(c) => c.encode_aof(ctx).await,
        }
    }
}

use crate::error::{ExecCommand, Frame, MultiCommand};

impl AofEncoder for MultiCommand {
    async fn encode_aof<'a>(&self, ctx: AofContent<'a>) -> Result<(), String> {
        let frame = Frame::Array(vec![Frame::Bulk(Bytes::from("MULTI"))]);
        ctx.aof_tx
            .send(frame.serialize())
            .await
            .map_err(|e| e.to_string())
    }
}

impl AofEncoder for ExecCommand {
    async fn encode_aof<'a>(&self, ctx: AofContent<'a>) -> Result<(), String> {
        let frame = Frame::Array(vec![Frame::Bulk(Bytes::from("EXEC"))]);
        ctx.aof_tx
            .send(frame.serialize())
            .await
            .map_err(|e| e.to_string())
    }
}

#[derive(Clone, Debug)]
pub struct AofContent<'a> {
    pub aof_tx: &'a Sender<Vec<u8>>,
    pub shutdown_tx: &'a tokio::sync::broadcast::Sender<()>,
}

pub fn exchange_absolute_time(expire_time: u64) -> Bytes {
    parse_int_from_bytes(get_cached_time_ms() + expire_time)
}

//高效的int 转byte 方法
pub fn parse_int_from_bytes(i: u64) -> Bytes {
    let mut buffer = Buffer::new();

    // 2. 将数字格式化到缓冲区中，返回一个指向缓冲区内容的 &str
    let printed_str = buffer.format(i);

    // 3. 从结果切片创建 Bytes (这里有一次复制，但避免了堆分配)
    Bytes::copy_from_slice(printed_str.as_bytes())
}
