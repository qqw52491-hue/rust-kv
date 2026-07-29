use crate::context::ConnectionContent;
use crate::core_execute::execute_command_normal;
use crate::core_explain::parse_frame;
use crate::db::Db;
use crate::error::{Command, Frame};
use bytes::{Buf, BytesMut};
use std::error::Error;
use std::io::IoSlice;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

// 1. 我们先定义一个“统一”的返回类型
enum ConnectionEvent {
    GotData(usize), // "获胜者"是“数据”，usize 是字节数
    Shutdown,       // "获胜者"是“关闭信号”
    ClientClosed,   // "获胜者"是“客户端自己关了”
}

// 处理单个客户端连接的函数
pub async fn handle_connection(
    mut socket: TcpStream,
    mut db: Db,
    mut connection_content: ConnectionContent,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // 禁用 Nagle 算法，避免在高并发 Pipelining 时出现延迟 ACK 导致的死锁/性能急剧下降
    let _ = socket.set_nodelay(true);
    // 1. 使用足够大的 Vec<u8> 作为缓冲区 (64KB)，确保 -P 32 等大批量命令能在一个 read_buf 内读取完毕
    let mut buf = BytesMut::with_capacity(1024 * 64);
    // 目前来说用的模式是1 是 redis 格式 0 是单个字符模式
    let type_fix = 1;
    //创建订阅者
    let mut receiver = connection_content.shutdown_tx.clone().subscribe();
    // 4. 在该连接的循环中读取数据
    'connection_loop: loop {
        let event = tokio::select! {
            res = socket.read_buf(&mut buf) =>{
                let n = res?; // 如果有 I/O 错误，? 会让函数提前 return Err
                if n == 0 {
                    // 客户端主动关闭
                    ConnectionEvent::ClientClosed
                } else {
                    // 成功读到 n 字节数据
                    ConnectionEvent::GotData(n)
                }
            }
            _ = receiver.recv() =>{
                    ConnectionEvent::Shutdown
            }
        };

        match event {
            ConnectionEvent::GotData(n) => {
                if type_fix == 0 {
                    if let Some(index) = find_crlf_idiomatic(&buf) {
                        println!("{}", index);
                        // try_parse_command_RESP(&buf[..index], &mut socket).expect("命令处理错误");

                        println!("接收到 {} 字节:  {:?}", n, &buf[..n]);

                        // 5. 【Echo 逻辑】将收到的数据原封不动写回给客户端
                        socket.write_all(&buf[..index + 2]).await?;

                        buf.advance(index + 2);
                    }
                } else {
                    if is_psync_request(&buf) {
                        // 这是一个从节点 (Slave) 发起的复制握手，移交套接字给 Replication Hub 负责推流
                        return crate::replication::handle_slave_psync(
                            socket,
                            connection_content.shutdown_tx,
                        )
                        .await;
                    }
                    match explain_execute_command(&mut buf, &mut db, &mut connection_content).await
                    {
                        Ok(result) => {
                            if result.len() == 1 {
                                // 非 pipeline 模式，直接发送首个响应，避免多余的 BytesMut 申请与数据拷贝
                                socket.write_all(&result[0]).await?;
                            } else if !result.is_empty() {
                                // pipeline 模式：采用 Vectored I/O (writev) 零拷贝向量化发送，彻底消除内存拷贝开销！
                                write_all_vectored(&mut socket, &result).await?;
                            }
                        }
                        Err(e) => {
                            // 转换失败（语义错误），准备一个错误响应
                            let error_response = Frame::Error(e.to_string());
                            socket.write_all(&error_response.serialize()).await?;
                            // 协议或命令格式已经损坏时无法可靠定位下一帧边界。
                            // 清空当前批次，避免坏数据永久滞留并导致缓冲区无限增长。
                            buf.clear();
                            // 继续处理缓冲区里的下一个命令
                            continue;
                        }
                    };
                }

                //println!("已回送数据");
            }
            ConnectionEvent::Shutdown => {
                println!("客户端主动关闭，退出循环。");
                break 'connection_loop;
            }
            ConnectionEvent::ClientClosed => {
                println!("收到关闭信号，退出循环。");
                // (在这里可以给客户端发一个最后的“告别”消息)
                break 'connection_loop;
            }
        }
    }
    Ok(())
}

async fn explain_execute_command(
    buf: &mut BytesMut,
    db: &mut Db,
    command_content: &mut ConnectionContent,
) -> Result<Vec<Vec<u8>>, Box<dyn Error + Send + Sync>> {
    let mut vec_result: Vec<Vec<u8>> = Vec::new();
    let mut vec: &[u8] = buf.as_ref();
    let mut total_size: usize = 0;
    /*
     * 首先盘点一下 由于分层 并且命令是字符串 所以每层都有可能出现错误
     * 1.第一层就是字符串解析成frame层 这个层面会出现的错误有 这个层面 只看是否能结构化成frame 和 具体指令要求无关
     *  1.解析过程中 首先就是发现命令没有传输完成就直接跳过
     *  2.发现比如格式错误
     *    1.中间/r/n没有
     *    2.字符串长度和实际标注不匹配
     *  第一层总体来说就是协议报错 是最底层的问题
     * 2.第二层就是frame 转换成command 这个就是要对于frame 生成结构严整
     *  1.首先就是遇到未知指令 返回直接返回说命令没有实现
     *  2.经典的命令长度不匹配 直接返回错误
     * 这一层是指令格式校验
     * 3.执行层面的话 这里错误比较少
     *   1.一半就是按照校验执行就行 执行出错的时候很少
     *   2.就是兼容没有实现的指令 这一步返回特定返回值 不需要再上一层就直接返回错误
     */
    loop {
        match parse_frame(vec) {
            Ok(Some((frame, size))) => match Command::try_from(frame) {
                Ok(command) => {
                    let result: Frame =
                        execute_command_normal(command, db, command_content.clone()).await?;
                    vec_result.push(result.serialize());
                    vec = &vec[size..];
                    total_size += size;
                }
                Err(e) => {
                    buf.advance(total_size);
                    return Err(e.into());
                }
            },
            Ok(None) => break,
            Err(e) => {
                buf.advance(total_size);
                return Err(e.into());
            }
        }
    }
    buf.advance(total_size);
    Ok(vec_result)
}
// 在缓冲区中查找 CRLF (`\r\n`) 的地道写法。
// 如果找到，返回 `\r` 的位置索引。
fn find_crlf_idiomatic(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|window| window == b"\r\n")
}

fn is_psync_request(buf: &[u8]) -> bool {
    let s = String::from_utf8_lossy(buf);
    s.contains("PSYNC") || s.contains("psync")
}

/// 使用 Linux writev (Vectored I/O) 实现的零拷贝批量写入辅助函数。
/// 将多个独立的响应 Memory Buffer 组合成 IoSlice 数组，内核直接按指针发送，避免用户态内存拷贝。
async fn write_all_vectored(
    socket: &mut TcpStream,
    bufs: &[Vec<u8>],
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut io_slices: Vec<IoSlice> = bufs.iter().map(|b| IoSlice::new(b)).collect();
    let mut slices = &mut io_slices[..];

    while !slices.is_empty() {
        let written = socket.write_vectored(slices).await?;
        if written == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write whole buffer using write_vectored",
            )
            .into());
        }
        IoSlice::advance_slices(&mut slices, written);
    }
    Ok(())
}
