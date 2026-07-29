use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::broadcast;
use tracing::{info, warn};

/// 全局 Master 主从复制广播 Hub (可存放最新的 10000 条写指令)
pub static REPLICATION_HUB: Lazy<broadcast::Sender<Vec<u8>>> = Lazy::new(|| {
    let (tx, _rx) = broadcast::channel(10000);
    tx
});

/// 已连接的从节点计数器
pub static SLAVE_COUNT: AtomicUsize = AtomicUsize::new(0);

/// 将已序列化的 RESP 写命令字节流广播给所有连入的从节点 (Slaves)
pub fn broadcast_bytes_to_slaves(cmd_bytes: Vec<u8>) {
    if REPLICATION_HUB.receiver_count() > 0 {
        let _ = REPLICATION_HUB.send(cmd_bytes);
    }
}

/// 增加一个从节点计数
pub fn add_slave_count() {
    let count = SLAVE_COUNT.fetch_add(1, Ordering::SeqCst) + 1;
    info!("新从节点 (Slave) 已建立连接，当前从节点总数: {}", count);
}

/// 减少一个从节点计数
pub fn sub_slave_count() {
    let count = SLAVE_COUNT.fetch_sub(1, Ordering::SeqCst).saturating_sub(1);
    warn!("从节点 (Slave) 连接断开，当前从节点总数: {}", count);
}
