use bytes::Bytes;
use crate::{
    aof_exchange::{AofContent, CommandAofExchange},
    error::{Frame, LPushCommand, LPopCommand},
};

impl CommandAofExchange for LPushCommand {
    async fn execute_aof<'a>(
        &self,
        ctx: AofContent<'a>,
    ) {
        let mut frame_vec = vec![Frame::Bulk(Bytes::from("LPUSH".to_string()))];
        frame_vec.push(Frame::Bulk(Bytes::from(self.key.to_string())));
        for val in &self.values {
            frame_vec.push(Frame::Bulk(val.clone()));
        }
        if let Err(e) = ctx.aof_tx.send(Frame::Array(frame_vec).serialize()).await {
            eprintln!("发送AOF消息失败: {}", e);
        }
    }
}

impl CommandAofExchange for LPopCommand {
    async fn execute_aof<'a>(
        &self,
        ctx: AofContent<'a>,
    ) {
        let mut frame_vec = vec![Frame::Bulk(Bytes::from("LPOP".to_string()))];
        frame_vec.push(Frame::Bulk(Bytes::from(self.key.to_string())));
        if let Err(e) = ctx.aof_tx.send(Frame::Array(frame_vec).serialize()).await {
            eprintln!("发送AOF消息失败: {}", e);
        }
    }
}
