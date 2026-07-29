use bytes::{Buf, BytesMut};
use std::error::Error;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::context::ConnectionContent;
use crate::core_execute::execute_command_normal;
use crate::core_explain::parse_frame;
use crate::db::Db;
use crate::error::Command;

/// 启动 Slave 角色的增量同步后台任务。
/// 会自动建立与 Master 的 TCP 连接，断线自动重连，并实时接收 Master 的写命令流在本地 DB 执行重放。
pub async fn start_slave_replication(
    master_addr: String,
    db: Db,
    shutdown_tx: broadcast::Sender<()>,
) {
    let mut shutdown_rx = shutdown_tx.subscribe();

    tokio::spawn(async move {
        info!("Slave 复制引擎启动，开始建立与 Master ({}) 的同步链路...", master_addr);

        'connect_loop: loop {
            // 检查是否收到了停机信号
            if shutdown_rx.try_recv().is_ok() {
                info!("Slave 复制引擎收到停机指令，安全退出。");
                break 'connect_loop;
            }

            // 1. 尝试连接 Master
            let mut socket = match TcpStream::connect(&master_addr).await {
                Ok(stream) => {
                    info!("成功连接至 Master 节点 ({})！开始同步数据...", master_addr);
                    stream
                }
                Err(e) => {
                    warn!("连接 Master ({}) 失败: {}。将在 3 秒后尝试重连...", master_addr, e);
                    tokio::select! {
                        _ = shutdown_rx.recv() => break 'connect_loop,
                        _ = tokio::time::sleep(Duration::from_secs(3)) => continue 'connect_loop,
                    }
                }
            };

            // 禁用 Nagle 算法以降低延迟
            let _ = socket.set_nodelay(true);

            // 发送握手 PING/PSYNC 消息告之 Master 自己是 Replica 节点
            let handshake_msg = b"*1\r\n$5\r\nPSYNC\r\n";
            if let Err(e) = socket.write_all(handshake_msg).await {
                error!("发送 PSYNC 握手指令失败: {}", e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue 'connect_loop;
            }

            // 构造模拟的 ConnectionContent 用于执行命令
            let dummy_conn_content = create_dummy_connection_content(shutdown_tx.clone());
            let mut buf = BytesMut::with_capacity(1024 * 64);

            // 2. 接收来自 Master 的命令数据流
            'stream_loop: loop {
                let read_res = tokio::select! {
                    res = socket.read_buf(&mut buf) => res,
                    _ = shutdown_rx.recv() => {
                        info!("Slave 收到关机通知，断开 Master 连接");
                        break 'connect_loop;
                    }
                };

                let n = match read_res {
                    Ok(n) if n == 0 => {
                        warn!("与 Master ({}) 的连接被对方关闭！即将重连...", master_addr);
                        break 'stream_loop;
                    }
                    Ok(n) => n,
                    Err(e) => {
                        error!("从 Master 读取数据流出错: {}。即将重连...", e);
                        break 'stream_loop;
                    }
                };

                // 3. 解析并重放来自于 Master 的写命令
                while let Ok(Some((frame, size))) = parse_frame(&buf) {
                    match Command::try_from(frame) {
                        Ok(cmd) => {
                            // 在 Slave 本地 DB 执行指令重放数据
                            if let Err(e) = execute_command_normal(cmd, &db, dummy_conn_content.clone()).await {
                                error!("Slave 执行 Master 重放命令失败: {}", e);
                            }
                        }
                        Err(e) => {
                            error!("Slave 解析 Master 指令失败: {}", e);
                        }
                    }
                    buf.advance(size);
                }
            }

            // 如果链接中断，等待 1 秒后重连
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
}

/// 为 Slave 重放逻辑生成一个基础的 ConnectionContent 占位符
fn create_dummy_connection_content(shutdown_tx: broadcast::Sender<()>) -> ConnectionContent {
    let (aof_tx, _) = tokio::sync::mpsc::channel(1);
    let (_lua_tx, lua_rx) = flume::unbounded();
    let lua_sender = crate::lua::lua_work::start_multi_lua_actor(1, 100);
    ConnectionContent {
        aof_tx,
        shutdown_tx,
        lua_sender,
        receivce_lua: lua_rx,
    }
}
