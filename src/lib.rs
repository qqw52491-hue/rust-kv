mod aof_exchange;
mod command_exchange;
mod command_execute;
mod config;
mod context;
mod core_aof;
mod core_exchange;
mod core_execute;
mod core_explain;
mod core_time;
mod db;
mod domain;
pub use crate::domain::error;
pub use crate::domain::types;
mod server;
mod shutdown;
mod lua;

use crate::config::CONFIG;
use crate::context::{CONN_STATE, ConnectionContent, ConnectionState};
use crate::core_aof::{AofMessage, aof_writer_task, explain_execute_aofcommand};
use crate::core_time::start_time_caching_task;
use crate::db::Db;
use crate::lua::lua_vm::init_lua_vm;
use crate::lua::lua_work::start_multi_lua_actor;
use crate::server::handle_connection;
use crate::shutdown::{ShutDown, shutdown_listener};
use mlua::Lua;
use tokio::task::JoinHandle;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{self};
use tokio::sync::{Mutex, broadcast};

/*
   各种服务的编排和关联
 */
pub async fn run() {
    // 这个通道必须要大 这个事最基本的事情
    let (aof_tx, rx) = mpsc::channel::<AofMessage>(1000000);
    //获取类型 这个广播
    let (app_shutdown_tx, _) = broadcast::channel::<()>(1);

    //地基停止 广播
    let (infra_shutdown_tx, _) = broadcast::channel::<()>(1);

    // 创建一个容量为 50 的“池”（通道）
    let (lua_vm_sender, lua_vm_receiver) = flume::bounded::<Lua>(50);

    //初始化lua 环境条件
    let (lua_runtime,lua_handle) = init_lua_vm(lua_vm_sender).await;

    //初始化并且直接获取sender
    let lua_sender = start_multi_lua_actor(8,100000);

    let aop_file_path = "database.aof";
    // 启动专门的 AOF 写入后台任务
    let aof_task = tokio::spawn(aof_writer_task(rx, aop_file_path, app_shutdown_tx.clone()));

    tracing_subscriber::fmt::init();
    // 1. 绑定监听地址
    // "127.0.0.1:6379" 是 Redis 的默认端口，我们沿用它可以方便地用 `redis-cli` 测试
    // 如果端口占用失败 直接报错退出
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    println!("服务器启动，监听于 127.0.0.1:6379");

    //创建db
    let mut db = Db::new(&CONFIG.eviction_type);
    // 模拟一个新的客户端连接进来
    let client_addr = "192.168.1.10:54321".to_string();
    let initial_state = ConnectionState {
        selected_db: 0, // 默认连接到 1 号数据库
        client_address: Some(client_addr)
    };
    CONN_STATE
        .scope(initial_state, async {
            match explain_execute_aofcommand(aop_file_path, &mut db).await {
                Err(e) => {
                    panic!("aof 清理失败  {}", e)
                }
                _ => {
                    println!("aof数据恢复成功")
                }
            }
        })
        .await;
    //开始时间获取任务
    let time_task = tokio::spawn(start_time_caching_task(infra_shutdown_tx.clone()));
    /*
     * db克隆代价很小
     * 同时开启两个异步任务
     * 1.过期时间检测淘汰
     * 2.内存监听淘汰
     * 都是定时任务执行到主线程结束
     */
    let eviction_ttl_task = tokio::spawn(db.clone().store.eviction_ttl(app_shutdown_tx.clone()));
    //多层task包裹方案 比较合适
    let eviction_memory_task: JoinHandle<Arc<Mutex<Vec<JoinHandle<()>>>>> = tokio::spawn(
        db.clone()
            .store
            .eviction_memory(1024 * 1024 * 8, app_shutdown_tx.clone()),
    );
    let connect_shutdown = app_shutdown_tx.clone();
    //包含任务队列
    let connect_task = tokio::spawn(async move {
        let connect_task_vec: Arc<Mutex<Vec<JoinHandle<()>>>> =
        Arc::new(Vec::new().into());
        // 2. 接受连接循环
        loop {
            let connect_content = ConnectionContent {
                aof_tx:aof_tx.clone(),
                shutdown_tx: connect_shutdown.clone(),
                lua_sender:lua_sender.clone(),
                receivce_lua:lua_vm_receiver.clone()
            };
            let mut receiver = connect_content.shutdown_tx.subscribe();
            // 等待一个新的客户端连接
            // 并不是包裹了一层 所以整体代码侵入行为降低
            // 现在整体等待被包裹成两个了
            let (socket, addr) = tokio::select! {
                res = listener.accept() =>{
                    match res {
                        Ok(res) => {
                            res
                        },
                        Err(_) => {
                            break;
                        },
                    }
                }
                _ = receiver.recv() =>{
                    break;
                }
            };
            // let (socket, _) = listener.accept().await;
            //tracing::info!("接收到新连接");
            let db = db.clone();

            let initial_state = ConnectionState {
                selected_db: 0, // 默认连接到 1 号数据库
                client_address: Some(addr.to_string())
            };
            // CONN_STATE
            //     .scope(initial_state, async {
            //         // 3. 为每个连接生成一个新的异步任务
            //         tokio::task::spawn(async move {
            //             // 在这个新任务中处理连接
            //             if let Err(e) = handle_connection(socket, db, tx_clone).await {
            //                 tracing::error!("处理时出错: {}", e);
            //             }
            //         });
            //     })
            //     .await;
            // 2. 【正确！】spawn 一个新任务
            let connect_task = tokio::task::spawn(async move {
                // 3. 【正确！】在新任务【内部】设置 TaskLocal
                CONN_STATE
                    .scope(initial_state, async move {
                        // 现在，这个 handle_connection 任务
                        // 以及它调用的所有函数 (比如 lock_read)
                        // 都可以安全地调用 CONN_STATE.with() 了！
                        if let Err(e) = handle_connection(socket, db, connect_content).await {
                            tracing::error!("处理时出错: {}", e);
                        }
                    })
                    .await; // .await 这个 scope
            });
            connect_task_vec.lock().await.push(connect_task);
        }
        connect_task_vec
    });
    let shutdown = ShutDown{
        aof_task,
        time_task,
        eviction_ttl_task,
        eviction_memory_task,
        connect_task,
        infra_shutdown_tx
    };
    //暂停收尾工作
    shutdown_listener(app_shutdown_tx).await;
    //收集关联后开启监听线程
    shutdown.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::error::{Command, Frame};
    use crate::command_execute::{CommandContext, CommandExecutor};
    use crate::context::{CONN_STATE, ConnectionState};
    use bytes::Bytes;

    #[tokio::test]
    async fn test_list_lpush_lpop() {
        let initial_state = ConnectionState {
            selected_db: 0,
            client_address: None,
        };

        CONN_STATE.scope(initial_state, async {
            // 1. 初始化数据库
            let db = Db::new(&crate::config::EvictionType::LRU);
            
            // 2. 用 Frame 模拟客户端发送 of LPUSH 请求，测试解析和转换层
            let lpush_frame = Frame::Array(vec![
                Frame::Bulk(Bytes::from("LPUSH")),
                Frame::Bulk(Bytes::from("mylist")),
                Frame::Bulk(Bytes::from("val1")),
                Frame::Bulk(Bytes::from("val2")),
            ]);
            
            let lpush_cmd = Command::try_from(lpush_frame).expect("解析 LPUSH 命令失败");
            
            // 3. 执行 LPUSH
            let ctx = CommandContext {
                db: Some(db.clone()),
                connect_content: None,
                lua_sessions: None,
            };
            let lpush_resp = lpush_cmd.execute(ctx)
                .await
                .expect("LPUSH 执行失败");
                
            // LPUSH 放入 2 个值后应该返回列表长度 2
            assert_eq!(lpush_resp, Frame::Integer(2));
            
            // 4. 模拟客户端发送的 LPOP 动作
            let lpop_frame = Frame::Array(vec![
                Frame::Bulk(Bytes::from("LPOP")),
                Frame::Bulk(Bytes::from("mylist")),
            ]);
            let lpop_cmd = Command::try_from(lpop_frame).expect("解析 LPOP 命令失败");
            
            // 5. 执行第一弹 LPOP
            // 因为 LPUSH 是从前部推入 (push_front) 元素，推入顺序是 "val1" 然后 "val2"
            // 最终列表状态应为: ["val2", "val1"]
            // 所以第一个 LPOP 出来的应该是 "val2"
            let ctx1 = CommandContext {
                db: Some(db.clone()),
                connect_content: None,
                lua_sessions: None,
            };
            let lpop_resp1 = lpop_cmd.execute(ctx1)
                .await
                .expect("LPOP 1 执行失败");
            assert_eq!(lpop_resp1, Frame::Bulk(Bytes::from("val2")));
            
            // 6. 执行 second LPOP
            let ctx2 = CommandContext {
                db: Some(db.clone()),
                connect_content: None,
                lua_sessions: None,
            };
            let lpop_resp2 = lpop_cmd.execute(ctx2)
                .await
                .expect("LPOP 2 执行失败");
            assert_eq!(lpop_resp2, Frame::Bulk(Bytes::from("val1")));
            
            // 7. 执行第三弹 LPOP (列表空，应该返回 Null)
            let ctx3 = CommandContext {
                db: Some(db.clone()),
                connect_content: None,
                lua_sessions: None,
            };
            let lpop_resp3 = lpop_cmd.execute(ctx3)
                .await
                .expect("LPOP 3 执行失败");
            assert_eq!(lpop_resp3, Frame::Null);
        }).await;
    }

    #[tokio::test]
    async fn test_hash_hset_hget_hdel() {
        let initial_state = ConnectionState {
            selected_db: 0,
            client_address: None,
        };

        CONN_STATE.scope(initial_state, async {
            let db = Db::new(&crate::config::EvictionType::LRU);
            
            // 1. HSET myhash field1 val1 field2 val2
            let hset_frame = Frame::Array(vec![
                Frame::Bulk(Bytes::from("HSET")),
                Frame::Bulk(Bytes::from("myhash")),
                Frame::Bulk(Bytes::from("field1")),
                Frame::Bulk(Bytes::from("val1")),
                Frame::Bulk(Bytes::from("field2")),
                Frame::Bulk(Bytes::from("val2")),
            ]);
            let hset_cmd = Command::try_from(hset_frame).expect("HSET parse fail");
            let ctx = CommandContext { db: Some(db.clone()), connect_content: None, lua_sessions: None };
            let hset_resp = hset_cmd.execute(ctx).await.expect("HSET exec fail");
            assert_eq!(hset_resp, Frame::Integer(2));

            // 2. HGET myhash field1
            let hget_frame = Frame::Array(vec![
                Frame::Bulk(Bytes::from("HGET")),
                Frame::Bulk(Bytes::from("myhash")),
                Frame::Bulk(Bytes::from("field1")),
            ]);
            let hget_cmd = Command::try_from(hget_frame.clone()).expect("HGET parse fail");
            let ctx2 = CommandContext { db: Some(db.clone()), connect_content: None, lua_sessions: None };
            let hget_resp = hget_cmd.execute(ctx2).await.expect("HGET exec fail");
            assert_eq!(hget_resp, Frame::Bulk(Bytes::from("val1")));

            // 3. HDEL myhash field1 field2
            let hdel_frame = Frame::Array(vec![
                Frame::Bulk(Bytes::from("HDEL")),
                Frame::Bulk(Bytes::from("myhash")),
                Frame::Bulk(Bytes::from("field1")),
                Frame::Bulk(Bytes::from("field2")),
            ]);
            let hdel_cmd = Command::try_from(hdel_frame).expect("HDEL parse fail");
            let ctx3 = CommandContext { db: Some(db.clone()), connect_content: None, lua_sessions: None };
            let hdel_resp = hdel_cmd.execute(ctx3).await.expect("HDEL exec fail");
            assert_eq!(hdel_resp, Frame::Integer(2));

            // 4. HGET myhash field1 again (should be null)
            let hget_cmd2 = Command::try_from(hget_frame).expect("HGET parse fail 2");
            let ctx4 = CommandContext { db: Some(db.clone()), connect_content: None, lua_sessions: None };
            let hget_resp2 = hget_cmd2.execute(ctx4).await.expect("HGET exec fail 2");
            assert_eq!(hget_resp2, Frame::Null);
        }).await;
    }
    #[tokio::test]
    async fn test_mset_mget() {
        let initial_state = ConnectionState {
            selected_db: 0,
            client_address: None,
        };

        CONN_STATE.scope(initial_state, async {
            let db = Db::new(&crate::config::EvictionType::LRU);
            
            // 1. MSET key1 val1 key2 val2
            let mset_frame = Frame::Array(vec![
                Frame::Bulk(Bytes::from("MSET")),
                Frame::Bulk(Bytes::from("key1")),
                Frame::Bulk(Bytes::from("val1")),
                Frame::Bulk(Bytes::from("key2")),
                Frame::Bulk(Bytes::from("val2")),
            ]);
            let mset_cmd = Command::try_from(mset_frame).expect("MSET parse fail");
            let ctx = CommandContext { db: Some(db.clone()), connect_content: None, lua_sessions: None };
            let mset_resp = mset_cmd.execute(ctx).await.expect("MSET exec fail");
            assert_eq!(mset_resp, Frame::Simple("OK".to_string()));

            // 2. GET key1
            let get_frame1 = Frame::Array(vec![
                Frame::Bulk(Bytes::from("GET")),
                Frame::Bulk(Bytes::from("key1")),
            ]);
            let get_cmd1 = Command::try_from(get_frame1).expect("GET parse fail");
            let ctx2 = CommandContext { db: Some(db.clone()), connect_content: None, lua_sessions: None };
            let get_resp1 = get_cmd1.execute(ctx2).await.expect("GET exec fail");
            assert_eq!(get_resp1, Frame::Bulk(Bytes::from("val1")));

            // 3. GET key2
            let get_frame2 = Frame::Array(vec![
                Frame::Bulk(Bytes::from("GET")),
                Frame::Bulk(Bytes::from("key2")),
            ]);
            let get_cmd2 = Command::try_from(get_frame2).expect("GET parse fail 2");
            let ctx3 = CommandContext { db: Some(db.clone()), connect_content: None, lua_sessions: None };
            let get_resp2 = get_cmd2.execute(ctx3).await.expect("GET exec fail 2");
            assert_eq!(get_resp2, Frame::Bulk(Bytes::from("val2")));
        }).await;
    }
}
