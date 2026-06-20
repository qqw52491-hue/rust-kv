use bytes::Bytes;
use crate::{
    aof_exchange::{AofContent, CommandAofExchange},
    error::{Frame, LPushCommand, LPopCommand},
};

impl CommandAofExchange for LPushCommand {
    async fn execute_aof<'a>(
        &self,
        ctx: AofContent<'a>,
    ) -> Result<(), String> {
        let mut frame_vec = vec![Frame::Bulk(Bytes::from_static(b"LPUSH"))];
        frame_vec.push(Frame::Bulk(Bytes::copy_from_slice(self.key.as_bytes())));
        for val in &self.values {
            frame_vec.push(Frame::Bulk(val.clone()));
        }
        ctx.aof_tx.send(Frame::Array(frame_vec).serialize()).await
            .map_err(|e| format!("发送AOF消息失败: {}", e))
    }
}

impl CommandAofExchange for LPopCommand {
    async fn execute_aof<'a>(
        &self,
        ctx: AofContent<'a>,
    ) -> Result<(), String> {
        let mut frame_vec = vec![Frame::Bulk(Bytes::from_static(b"LPOP"))];
        frame_vec.push(Frame::Bulk(Bytes::copy_from_slice(self.key.as_bytes())));
        ctx.aof_tx.send(Frame::Array(frame_vec).serialize()).await
            .map_err(|e| format!("发送AOF消息失败: {}", e))
    }
}
