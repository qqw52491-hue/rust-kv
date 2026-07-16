use bytes::Bytes;
use crate::{
    aof_encoder::{AofContent, AofEncoder},
    domain::command::JsonSetCommand,
    error::Frame,
};

impl AofEncoder for JsonSetCommand {
    async fn encode_aof<'a>(&self, ctx: AofContent<'a>) -> Result<(), String> {
        let mut frames = vec![
            Frame::Bulk(Bytes::from("JSON.SET")),
            Frame::Bulk(Bytes::from(self.key.as_bytes().to_vec())),
            Frame::Bulk(Bytes::from(self.path.clone())),
            Frame::Bulk(Bytes::from(self.value.clone())),
        ];

        let buf = Frame::Array(frames).serialize();
        ctx.aof_tx
            .send(buf)
            .await
            .map_err(|e| format!("Failed to send to aof_tx: {}", e))
    }
}
