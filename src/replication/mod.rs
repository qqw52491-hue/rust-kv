pub mod master;
pub mod slave;
pub mod election;

use std::error::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tracing::{info, warn};

use master::{add_slave_count, sub_slave_count, REPLICATION_HUB};

/// 当 Master 接收到 Slave 的 PSYNC 握手指令时调用的处理函数。
/// 该函数会把 Slave 注册到全局广播 Hub，并将后续的所有增量写指令推送到该 Slave 套接字中。
pub async fn handle_slave_psync(
    mut socket: TcpStream,
    shutdown_tx: broadcast::Sender<()>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    add_slave_count();
    let mut rx = REPLICATION_HUB.subscribe();
    let mut shutdown_rx = shutdown_tx.subscribe();

    // 先响应一个 +FULLRESYNC 开头标识
    let ok_resp = b"+FULLRESYNC 0000000000000000000000000000000000000000 0\r\n";
    let _ = socket.write_all(ok_resp).await;

    info!("从节点 (Slave) 鉴权成功，已启动实时写命令推流任务！");

    loop {
        tokio::select! {
            // 从广播 Channel 中拉取 Master 刚刚执行的写命令 Frame
            msg_res = rx.recv() => {
                match msg_res {
                    Ok(bytes) => {
                        if let Err(e) = socket.write_all(&bytes).await {
                            warn!("向从节点 (Slave) 发送命令流失败: {}，断开连接。", e);
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        warn!("Slave 推流落后，跳过了 {} 条指令", skipped);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                }
            }
            _ = shutdown_rx.recv() => {
                info!("Master 停机，关闭与 Slave 的推送连接");
                break;
            }
        }
    }

    sub_slave_count();
    Ok(())
}
