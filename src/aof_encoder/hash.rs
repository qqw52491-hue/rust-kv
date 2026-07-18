use crate::aof_encoder::{AofContent, AofEncoder};
use crate::domain::{HDelCommand, HSetCommand};

impl AofEncoder for HSetCommand {
    async fn encode_aof<'a>(&self, ctx: AofContent<'a>) -> Result<(), String> {
        let mut buf = Vec::new();
        // * (1 + 1 + field_values.len() * 2)
        let total_parts = 2 + self.field_values.len() * 2;
        buf.extend_from_slice(format!("*{}\r\n$4\r\nHSET\r\n", total_parts).as_bytes());
        buf.extend_from_slice(format!("${}\r\n", self.key.len()).as_bytes());
        buf.extend_from_slice(self.key.as_bytes());
        buf.extend_from_slice(b"\r\n");
        for (field, val) in &self.field_values {
            buf.extend_from_slice(format!("${}\r\n", field.len()).as_bytes());
            buf.extend_from_slice(field.as_bytes());
            buf.extend_from_slice(b"\r\n");

            buf.extend_from_slice(format!("${}\r\n", val.len()).as_bytes());
            buf.extend_from_slice(val);
            buf.extend_from_slice(b"\r\n");
        }
        ctx.aof_tx
            .send(buf)
            .await
            .map_err(|e| format!("发送AOF消息失败: {}", e))
    }
}

impl AofEncoder for HDelCommand {
    async fn encode_aof<'a>(&self, ctx: AofContent<'a>) -> Result<(), String> {
        let mut buf = Vec::new();
        // * (1 + 1 + fields.len())
        let total_parts = 2 + self.fields.len();
        buf.extend_from_slice(format!("*{}\r\n$4\r\nHDEL\r\n", total_parts).as_bytes());
        buf.extend_from_slice(format!("${}\r\n", self.key.len()).as_bytes());
        buf.extend_from_slice(self.key.as_bytes());
        buf.extend_from_slice(b"\r\n");
        for field in &self.fields {
            buf.extend_from_slice(format!("${}\r\n", field.len()).as_bytes());
            buf.extend_from_slice(field.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        ctx.aof_tx
            .send(buf)
            .await
            .map_err(|e| format!("发送AOF消息失败: {}", e))
    }
}
