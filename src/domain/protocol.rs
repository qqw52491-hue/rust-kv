use bytes::Bytes;

// ─────────────────────────────────────────────
// RESP 协议帧：客户端与服务端之间的通信单元
// ─────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    Simple(String),
    Bulk(Bytes),
    Array(Vec<Frame>),
    Integer(i64),
    Null,
    Error(String),
}

// ─────────────────────────────────────────────
// 辅助枚举（暂未使用，为后续序列化预留）
// ─────────────────────────────────────────────

pub enum ToBulk {
    String(String),
    Btyes(Bytes),
    Integer(i64),
}

pub enum IsAof {
    Yes,
    No,
}
