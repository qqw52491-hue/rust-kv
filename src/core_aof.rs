use std::error::Error;
use std::fs::File;

use std::io::Read;
use tokio::fs::OpenOptions;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc::Receiver;
use tokio::time::{self, Duration};

use crate::core_execute::{ execute_command};
use crate::core_explain::parse_frame;
use crate::error::{Command};
use crate::Db;


// 定义管道里传递的消息类型，这里就是序列化后的命令
pub type AofMessage = Vec<u8>;


pub async fn aof_writer_task(mut rx: Receiver<AofMessage>, path: &str, sender: Sender<()>) {
    // 打开 AOF 文件
    let mut file = BufWriter::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await
            .unwrap(),
    );

    // 1. 初始化缓冲区 (只分配一次内存，复用)
    let mut buffer: Vec<AofMessage> = Vec::with_capacity(5000); 
    // 2. 订阅停机信号
    let mut shutdown_rx = sender.subscribe();

    'main_loop: loop {
        // 3. 【核心修改】同时等待“新数据”和“停机信号”
        //    谁先来处理谁，不会傻等
        let first_msg = tokio::select! {
            // 情况 A: 收到数据
            res = rx.recv() => {
                match res {
                    Some(msg) => msg,
                    None => break 'main_loop, // 发送端彻底关闭
                }
            },
            // 情况 B: 收到停机信号
            _ = shutdown_rx.recv() => {
                println!("AOF 任务收到停机信号，准备停止...");
                break 'main_loop; // 跳出循环，去执行下面的收尾
            }
        };

        // --- 正常处理逻辑 ---
        
        // 先存入第一条
        buffer.push(first_msg);

        // 4. 【贪婪批处理】趁热打铁
        //    看看通道里是不是还积压了一堆？有的话全捞出来 (最多捞5000条防止卡死)
        while buffer.len() < 5000 {
            match rx.try_recv() {
                Ok(msg) => buffer.push(msg),
                Err(_) => break, // 通道暂时空了，别等了，赶紧写盘
            }
        }

        // 5. 批量落盘
        if !buffer.is_empty() {
            for msg in &buffer {
                if let Err(e) = file.write_all(msg).await {
                    tracing::error!("AOF 写入失败: {}", e);
                }
            }
            // 必须 flush 确保数据真正进入磁盘
            if let Err(e) = file.flush().await {
                tracing::error!("AOF 刷盘失败: {}", e);
            }
            
            // 6. 【关键】写完再清空，复用容量
            buffer.clear(); 
        }
    }

    // --- 7. 【安全着陆】停机收尾逻辑 ---
    // 循环跳出后，通道里可能还残留着几百条数据，必须写完再走！
    println!("AOF 正在执行最后的数据落盘 (Draining)...");
    
    // 把剩下的全捞出来
    while let Ok(msg) = rx.try_recv() {
        buffer.push(msg);
    }
    
    // 最后一次写入
    if !buffer.is_empty() {
        for msg in &buffer {
            let _ = file.write_all(msg).await;
        }
        let _ = file.flush().await;
    }
    
    println!("AOF 任务已安全退出，数据零丢失。");
}

pub async fn explain_execute_aofcommand(
    path: &str,
    db: & mut Db,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut file = File::open(path)?;
    //单线程恢复可以很大
    let mut file_data: Vec<u8> = vec![0; 1024 * 1024 * 512];
    let mut exec_time;
    let mut tail_file_length = 0;
    //这个默认恢复从0 开始 
    //let mut conn_state = ConnectionState { selected_db: 0 ,client_address: None};
    let mut transaction_buffer: Option<Vec<Command>> = None; // 【新增】事务缓冲，用于防撕裂
    loop {
        let size: usize = file.read(&mut file_data[tail_file_length..])? + tail_file_length;
        let mut tail_size: usize = 0;
        let mut data: &[u8] = &file_data[0..size];
        
        exec_time = 0;
        if size == 0 {
            break;
        }

        loop {
            if data.len() == 0 {
                break; // 如果刚好把数据完整读完，应该 break 去外层继续读文件，而不是直接 return 结束
            }
            match parse_frame(data) {
                Ok(frame) => match frame {
                    //这个分支只有不可变
                    Some((frame, frame_size)) => { // 重命名为 frame_size 避免遮蔽外层的 size
                        match Command::try_from(frame) {
                            Ok(cmd) => {
                                match cmd {
                                    Command::Multi(_) => {
                                        transaction_buffer = Some(Vec::new());
                                    }
                                    Command::Exec(_) => {
                                        if let Some(cmds) = transaction_buffer.take() {
                                            for buffered_cmd in cmds {
                                                execute_command(buffered_cmd, db).await?;
                                            }
                                        }
                                    }
                                    _ => {
                                        if let Some(buffer) = &mut transaction_buffer {
                                            buffer.push(cmd);
                                        } else {
                                            execute_command(cmd, db).await?;
                                        }
                                    }
                                }
                                data = &data[frame_size..];
                                exec_time += 1;
                            }
                            Err(e) => {
                                return Err(e.into());
                            }
                        };
                        tail_size += frame_size; // 这里必须是累加，记录当前这批数据一共消耗了多少字节
                    }
                    //这个分支有可变的复制 但是并没有使用可变 但是直接使用了对象  对象 不可变 可变三者交叉使用
                    //只要每个域都没有问题 就可以交叉使用 没有问题
                    None => {
                        file_data.copy_within(tail_size..size, 0);
                        // 5. 将剩下的数据移动到缓冲区头部
                        tail_file_length = size - tail_size;
                        if exec_time == 0 {
                            return Err("字符串文件过大 大于512M".into());
                        }
                        break;
                    }
                },
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    }

    
    // 【新增】如果文件读取结束了，但 transaction_buffer 里还有东西，说明遇到了断电撕裂！
    // 没遇到 EXEC，我们直接丢弃这批数据，完成完美回滚。
    if let Some(unfinished) = transaction_buffer {
        println!("警告: 发现断电撕裂导致的不完整事务！已自动回滚 {} 条未提交命令。", unfinished.len());
    }

    Ok(())
}
