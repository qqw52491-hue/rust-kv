use crate::{
    aof_encoder::{AofContent, AofEncoder},
    domain::command::{ZAddCommand, ZRemCommand},
    error::Frame,
};
use bytes::Bytes;

impl AofEncoder for ZAddCommand {
    async fn encode_aof<'a>(&self, ctx: AofContent<'a>) -> Result<(), String> {
        let frames = vec![
            Frame::Bulk(Bytes::from("ZADD")),
            Frame::Bulk(Bytes::from(self.key.as_bytes().to_vec())),
            Frame::Bulk(Bytes::from(self.score.to_string())),
            Frame::Bulk(self.member.clone()),
        ];

        let buf = Frame::Array(frames).serialize();
        ctx.aof_tx
            .send(buf)
            .await
            .map_err(|e| format!("Failed to send to aof_tx: {}", e))
    }
}

impl AofEncoder for ZRemCommand {
    async fn encode_aof<'a>(&self, ctx: AofContent<'a>) -> Result<(), String> {
        let frames = vec![
            Frame::Bulk(Bytes::from("ZREM")),
            Frame::Bulk(Bytes::from(self.key.as_bytes().to_vec())),
            Frame::Bulk(self.member.clone()),
        ];

        let buf = Frame::Array(frames).serialize();
        ctx.aof_tx
            .send(buf)
            .await
            .map_err(|e| format!("Failed to send to aof_tx: {}", e))
    }
}
