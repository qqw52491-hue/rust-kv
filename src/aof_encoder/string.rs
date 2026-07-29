use bytes::Bytes;
use futures::SinkExt;

use crate::{
    aof_encoder::{AofContent, AofEncoder, exchange_absolute_time, parse_int_from_bytes},
    error::{Command, Frame, MSetCommand, SetCommand},
};

impl AofEncoder for SetCommand {
    async fn encode_aof<'a>(
        &self,
        // 2. 将这个生命周期 'ctx 应用到 CommandContext 的引用上
        ctx: AofContent<'a>,
    ) -> Result<(), String> {
        let mut frame_vec = vec![crate::error::Frame::Bulk(Bytes::from_static(b"SET"))];
        frame_vec.push(crate::error::Frame::Bulk(Bytes::copy_from_slice(
            self.key.as_bytes(),
        )));
        frame_vec.push(crate::error::Frame::Bulk(Bytes::from(self.value.clone())));
        if let Some(expire) = &self.expiration {
            match expire {
                crate::error::Expiration::EX(s) => {
                    frame_vec.push(crate::error::Frame::Bulk(Bytes::from_static(b"EXAT")));
                    let expire_bytes = exchange_absolute_time(s * 1000);
                    frame_vec.push(crate::error::Frame::Bulk(expire_bytes));
                }
                crate::error::Expiration::PX(ms) => {
                    frame_vec.push(crate::error::Frame::Bulk(Bytes::from_static(b"PXAT")));
                    let expire_bytes = exchange_absolute_time(ms.clone());
                    frame_vec.push(crate::error::Frame::Bulk(expire_bytes));
                }
                crate::error::Expiration::EXAT(s) => {
                    frame_vec.push(crate::error::Frame::Bulk(Bytes::from_static(b"EXAT")));
                    let expire_bytes = parse_int_from_bytes(s.clone());
                    frame_vec.push(crate::error::Frame::Bulk(expire_bytes));
                }
                crate::error::Expiration::PXAT(ms) => {
                    frame_vec.push(crate::error::Frame::Bulk(Bytes::from_static(b"PXAT")));
                    let expire_bytes = parse_int_from_bytes(ms.clone());
                    frame_vec.push(crate::error::Frame::Bulk(expire_bytes));
                }
            }
        }
        ctx.aof_tx
            .send(Frame::Array(frame_vec).serialize())
            .await
            .map_err(|e| format!("发送AOF消息失败: {}", e))
    }
}

impl AofEncoder for MSetCommand {
    async fn encode_aof<'a>(&self, ctx: AofContent<'a>) -> Result<(), String> {
        let mut frame_vec = vec![crate::error::Frame::Bulk(Bytes::from_static(b"MSET"))];
        for (key, val) in &self.keys_and_values {
            frame_vec.push(crate::error::Frame::Bulk(Bytes::copy_from_slice(
                key.as_bytes(),
            )));
            frame_vec.push(crate::error::Frame::Bulk(val.clone()));
        }
        ctx.aof_tx
            .send(Frame::Array(frame_vec).serialize())
            .await
            .map_err(|e| format!("发送AOF消息失败: {}", e))
    }
}
