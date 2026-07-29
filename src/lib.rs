mod aof_encoder;
mod config;
mod context;
mod core_aof;
mod core_exchange;
mod core_execute;
mod core_explain;
mod core_time;
mod db;
mod domain;
mod executor;
mod parser;
pub use crate::domain::error;
pub use crate::domain::types;
mod lua;
pub mod replication;
mod server;
mod shutdown;

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
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{self};
use tokio::sync::{Mutex, broadcast};
use tokio::task::JoinHandle;
use std::time::Duration;
use crate::db::eviction::GLOBAL_MEMORY;
use std::sync::atomic::Ordering;

/*
  各种服务的编排和关联
*/
pub async fn run() {
    // 启动 Prometheus Exporter 后台 HTTP 服务器 (监听 9091 端口，避开 Prometheus 自身的 9090)
    metrics_exporter_prometheus::PrometheusBuilder::new()
        .with_http_listener(([0, 0, 0, 0], 9091))
        .install()
        .expect("Failed to install Prometheus recorder");
    println!("监控模块启动，Prometheus 指标接口: http://0.0.0.0:9091/metrics");

    // 启动一个后台任务，专门负责大屏里的“内存图表”
    tokio::spawn(async move {
        loop {
            // 每隔 1 秒，把当前的全局内存同步给 Prometheus
            let mem = GLOBAL_MEMORY.load(Ordering::Relaxed);
            metrics::gauge!("kv_total_memory_bytes").set(mem as f64);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    // 这个通道必须要大 这个事最基本的事情
    let (aof_tx, rx) = mpsc::channel::<AofMessage>(1000000);
    //获取类型 这个广播
    let (app_shutdown_tx, _) = broadcast::channel::<()>(1);

    //地基停止 广播
    let (infra_shutdown_tx, _) = broadcast::channel::<()>(1);

    // 创建一个容量为 lua_vm_pool_size 的“池”（通道）
    let (lua_vm_sender, lua_vm_receiver) = flume::bounded::<Lua>(CONFIG.lua_vm_pool_size);

    //初始化lua 环境条件
    let (lua_runtime, _lua_handle) = init_lua_vm(lua_vm_sender).await;

    //初始化并且直接获取sender
    let lua_sender = start_multi_lua_actor(CONFIG.lua_worker_count, CONFIG.lua_queue_depth);

    // 启动专门的 AOF 写入后台任务
    let aof_task = tokio::spawn(aof_writer_task(rx, &CONFIG.aof_file_path));

    tracing_subscriber::fmt::init();
    // 1. 绑定监听地址
    let listener = TcpListener::bind(&CONFIG.server_addr).await.unwrap();
    println!("服务器启动，监听于 {}", CONFIG.server_addr);

    //创建db
    let mut db = Db::new(&CONFIG.eviction_type);
    // 模拟一个新的客户端连接进来
    let client_addr = "192.168.1.10:54321".to_string();
    let initial_state = ConnectionState {
        selected_db: 0, // 默认连接到 1 号数据库
        client_address: Some(client_addr),
    };
    CONN_STATE
        .scope(initial_state, async {
            match explain_execute_aofcommand(&CONFIG.aof_file_path, &mut db).await {
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
            .eviction_memory(1024 * 1024 * 80, app_shutdown_tx.clone()),
    );

    // 如果配置了 replica_of，自动启动 Slave 与 Master 之间的实实时增量复制链路
    if let Some(ref master_addr) = CONFIG.replica_of {
        replication::slave::start_slave_replication(
            master_addr.clone(),
            db.clone(),
            app_shutdown_tx.clone(),
        )
        .await;
    }

    // 启动分布式集群的“造反炸弹”心跳倒计时，维护 Raft 选举机制
    crate::replication::election::start_election_loop();

    let connect_shutdown = app_shutdown_tx.clone();
    //包含任务队列
    let connect_task = tokio::spawn(async move {
        let connect_task_vec: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Vec::new().into());
        // 2. 接受连接循环
        loop {
            let connect_content = ConnectionContent {
                aof_tx: aof_tx.clone(),
                shutdown_tx: connect_shutdown.clone(),
                lua_sender: lua_sender.clone(),
                receivce_lua: lua_vm_receiver.clone(),
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
                client_address: Some(addr.to_string()),
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
    let shutdown = ShutDown {
        aof_task,
        time_task,
        eviction_ttl_task,
        eviction_memory_task,
        connect_task,
        infra_shutdown_tx,
    };
    //暂停收尾工作
    shutdown_listener(app_shutdown_tx).await;
    // 收集关联后开启监听线程
    shutdown.shutdown().await;

    // 优雅地清理 Lua Runtime（防止在 async 境下直接 drop 导致报错）
    tokio::task::spawn_blocking(move || {
        drop(lua_runtime);
    })
    .await
    .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::{CONN_STATE, ConnectionState};
    use crate::db::Db;
    use crate::error::{Command, Frame};
    use crate::executor::{CommandContext, Executor};
    use bytes::Bytes;

    #[tokio::test]
    async fn test_list_lpush_lpop() {
        let initial_state = ConnectionState {
            selected_db: 0,
            client_address: None,
        };

        CONN_STATE
            .scope(initial_state, async {
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
                let ctx = CommandContext::Recovery { db: db.clone() };
                let lpush_resp = lpush_cmd.execute(ctx).await.expect("LPUSH 执行失败");

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
                let ctx1 = CommandContext::Recovery { db: db.clone() };
                let lpop_resp1 = lpop_cmd.execute(ctx1).await.expect("LPOP 1 执行失败");
                assert_eq!(lpop_resp1, Frame::Bulk(Bytes::from("val2")));

                // 6. 执行 second LPOP
                let ctx2 = CommandContext::Recovery { db: db.clone() };
                let lpop_resp2 = lpop_cmd.execute(ctx2).await.expect("LPOP 2 执行失败");
                assert_eq!(lpop_resp2, Frame::Bulk(Bytes::from("val1")));

                // 7. 执行第三弹 LPOP (列表空，应该返回 Null)
                let ctx3 = CommandContext::Recovery { db: db.clone() };
                let lpop_resp3 = lpop_cmd.execute(ctx3).await.expect("LPOP 3 执行失败");
                assert_eq!(lpop_resp3, Frame::Null);
            })
            .await;
    }

    #[tokio::test]
    async fn test_hash_hset_hget_hdel() {
        let initial_state = ConnectionState {
            selected_db: 0,
            client_address: None,
        };

        CONN_STATE
            .scope(initial_state, async {
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
                let ctx = CommandContext::Recovery { db: db.clone() };
                let hset_resp = hset_cmd.execute(ctx).await.expect("HSET exec fail");
                assert_eq!(hset_resp, Frame::Integer(2));

                // 2. HGET myhash field1
                let hget_frame = Frame::Array(vec![
                    Frame::Bulk(Bytes::from("HGET")),
                    Frame::Bulk(Bytes::from("myhash")),
                    Frame::Bulk(Bytes::from("field1")),
                ]);
                let hget_cmd = Command::try_from(hget_frame.clone()).expect("HGET parse fail");
                let ctx2 = CommandContext::Recovery { db: db.clone() };
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
                let ctx3 = CommandContext::Recovery { db: db.clone() };
                let hdel_resp = hdel_cmd.execute(ctx3).await.expect("HDEL exec fail");
                assert_eq!(hdel_resp, Frame::Integer(2));

                // 4. HGET myhash field1 again (should be null)
                let hget_cmd2 = Command::try_from(hget_frame).expect("HGET parse fail 2");
                let ctx4 = CommandContext::Recovery { db: db.clone() };
                let hget_resp2 = hget_cmd2.execute(ctx4).await.expect("HGET exec fail 2");
                assert_eq!(hget_resp2, Frame::Null);
            })
            .await;
    }
    #[tokio::test]
    async fn test_mset_mget() {
        let initial_state = ConnectionState {
            selected_db: 0,
            client_address: None,
        };

        CONN_STATE
            .scope(initial_state, async {
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
                let ctx = CommandContext::Recovery { db: db.clone() };
                let mset_resp = mset_cmd.execute(ctx).await.expect("MSET exec fail");
                assert_eq!(mset_resp, Frame::Simple("OK".to_string()));

                // 2. GET key1
                let get_frame1 = Frame::Array(vec![
                    Frame::Bulk(Bytes::from("GET")),
                    Frame::Bulk(Bytes::from("key1")),
                ]);
                let get_cmd1 = Command::try_from(get_frame1).expect("GET parse fail");
                let ctx2 = CommandContext::Recovery { db: db.clone() };
                let get_resp1 = get_cmd1.execute(ctx2).await.expect("GET exec fail");
                assert_eq!(get_resp1, Frame::Bulk(Bytes::from("val1")));

                // 3. GET key2
                let get_frame2 = Frame::Array(vec![
                    Frame::Bulk(Bytes::from("GET")),
                    Frame::Bulk(Bytes::from("key2")),
                ]);
                let get_cmd2 = Command::try_from(get_frame2).expect("GET parse fail 2");
                let ctx3 = CommandContext::Recovery { db: db.clone() };
                let get_resp2 = get_cmd2.execute(ctx3).await.expect("GET exec fail 2");
                assert_eq!(get_resp2, Frame::Bulk(Bytes::from("val2")));
            })
            .await;
    }
}
