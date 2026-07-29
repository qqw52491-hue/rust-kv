use mlua::prelude::*;
use std::{cell::RefCell, collections::HashMap, sync::Arc, thread};
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::{
    context::{CONN_STATE, ConnectionContent, ConnectionState},
    db::{Db, LockedDb},
    error::{EvalCommand, Frame, KvError},
    executor::CommandContext,
    lua::lua_vm::{general_lua, init_lua_pre},
};

// 一个请求包含：上下文参数 + 回信地址
pub struct LuaTask {
    pub ctx: CommandContext, // 你的参数
    pub resp: oneshot::Sender<Result<Frame, KvError>>,
    pub command: EvalCommand,
    pub connect_state: ConnectionState,
}

// 这里啊是为了提前操作lua 所以需要的结构
pub struct CurrentRequestEnv {
    pub ctx: CommandContext,                            // 你的环境
    pub sessions: Arc<Mutex<HashMap<usize, LockedDb>>>, // 你的锁 (直接用 HashMap，不需要 Arc Mutex)
    pub command: EvalCommand,
    pub lua_aof_buffer: Vec<crate::error::Command>, // 【新增】存放执行成功的命令，用于 AOF 效果同步
}

thread_local! {
    pub static CURRENT_ENV: RefCell<Option<CurrentRequestEnv>> = RefCell::new(None);
}

#[derive(Clone)]
pub struct LuaRouter {
    // 存放所有工人的通道
    pub senders: Vec<mpsc::Sender<LuaTask>>,
}

impl LuaRouter {
    pub async fn dispatch(&self, task: LuaTask) -> Result<(), KvError> {
        let n = self.senders.len();
        if n == 0 {
            return Err(KvError::ProtocolError("Lua 引擎过载或已关闭".into()));
        }

        // 随机起点，避免容量相同时永远偏向 index 0
        let offset = rand::random::<usize>() % n;
        let mut best_index = offset;
        let mut max_capacity = self.senders[offset].capacity();

        for k in 1..n {
            let i = (offset + k) % n;
            let cap = self.senders[i].capacity();
            if cap > max_capacity {
                max_capacity = cap;
                best_index = i;
            }
        }

        self.senders[best_index]
            .send(task)
            .await
            .map_err(|_| KvError::ProtocolError("Lua 引擎过载或已关闭".into()))
    }
}
// 启动一个 Lua 专用线程，返回它的“传菜口”(Sender)
// 启动函数稍微调整一下，不再需要 AtomicUsize 了
pub fn start_multi_lua_actor(worker_num: usize, queue_size: usize) -> LuaRouter {
    let mut senders = Vec::with_capacity(worker_num);

    for i in 0..worker_num {
        let (tx, mut rx) = mpsc::channel::<LuaTask>(queue_size);
        senders.push(tx);
        thread::spawn(move || {
            //独占环境
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            //单线程声明
            let local = tokio::task::LocalSet::new();

            println!("Lua Worker #{} (智能负载) 启动就绪", i);

            //这里才开始执行单线程内容
            local.block_on(&rt, async move {
                //生成lua 初始化内部内容 开始绑定
                let lua = general_lua().await.unwrap();
                while let Some(task) = rx.recv().await {
                    let sender = task.resp;
                    let result = CONN_STATE
                        .scope(task.connect_state, async {
                            init_lua_pre(&lua, &task.command, task.ctx).await;
                            EvalCommand::lua_vm_redis_call(&(task.command), &lua).await
                        })
                        .await;
                    let _ = sender.send(result);
                }
            });
        });
    }

    LuaRouter {
        senders,
        // counter 删掉了，不需要了
    }
}
