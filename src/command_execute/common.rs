use bytes::Bytes;

use crate::{
    command_execute::{CommandContext, CommandExecutor},
    context::{CONN_STATE, ConnectionState},
    db::LockedDb,
    error::{EvalCommand, ExecCommand, Frame, KvError, MultiCommand, PingCommand, UnimplementCommand},
    lua::lua_work::LuaTask,
};
use tokio::sync::oneshot;
impl CommandExecutor for PingCommand {
    async fn execute(
        &self,
        _ctx: CommandContext,
    ) -> Result<Frame, KvError> {
        if let Some(value) = &self.value {
            Ok(Frame::Bulk(Bytes::from(value.clone())))
        } else {
            Ok(Frame::Simple("PONG".into()))
        }
    }
}

impl CommandExecutor for UnimplementCommand {
    async fn execute(
        &self,
        _ctx: CommandContext,
    ) -> Result<Frame, KvError> {
        Ok(Frame::Error(format!(
            "ERR unknown command '{}'",
            self.command
        )))
    }
}

/*
这个是比较特殊的执行层
*/
impl CommandExecutor for EvalCommand {
    async fn execute(
        &self,
        ctx: CommandContext,
    ) -> Result<Frame, KvError> {
        //   let result =   self.lua_vm_redis_call(
        // CommandContext {
        //     db: ctx.db.clone(),
        //     connect_content: ctx.connect_content.clone(),
        // }).await; // 直接 await！
        //现在我复制了这个链接
        let content = match &ctx {
            CommandContext::Normal { connect_content, .. } => connect_content.clone(),
            _ => return Err(KvError::ProtocolError("Eval can only run in a normal client context".to_string())),
        };

        // 这里的 Result<Frame, KvError> 就是你要通过信封回传的数据类型
        let (tx, rx) = oneshot::channel::<Result<Frame, KvError>>();

        //这一步记得传递上下文
        content
            .lua_sender
            .dispatch(LuaTask {
                ctx: ctx.clone(),
                resp: tx,
                command: self.clone(),
                connect_state: ConnectionState {
                    selected_db: CONN_STATE.with(|state| state.selected_db),
                    client_address: None,
                },
            })
            .await // <--- 关键！驱动发送动作
            .map_err(|_| KvError::ProtocolError("Lua Worker 已挂掉".into()))?;

        // 1. 先等待通道回信
        let channel_result = rx.await;

        // 2. 检查通道是否正常
        let result = match channel_result {
            Ok(inner_result) => {
                // 通道正常，拿到里面的 Result<Frame, KvError>
                //println!("通道接收成功，Lua 执行结果: {:?}", inner_result);
                inner_result
                // 如果你需要把 inner_result 赋值给 result 变量
                // let result = inner_result;
            }
            Err(e) => {
                // 💥 捕捉到了！打印错误！
                // 这里的 e 是 tokio::sync::oneshot::error::RecvError
                eprintln!("CRITICAL ERROR: Lua 线程没回信就挂了！错误信息: {:?}", e);
                Err(KvError::ProtocolError("wtf".into()))
                // 这里你可以返回一个 System Error 给客户端
                // return Err(KvError::String("Internal Lua Thread Error".into()));
            }
        };
        result
    }
}

impl CommandExecutor for MultiCommand {
    async fn execute(&self, _ctx: CommandContext) -> Result<Frame, KvError> {
        Ok(Frame::Null)
    }
}

impl CommandExecutor for ExecCommand {
    async fn execute(&self, _ctx: CommandContext) -> Result<Frame, KvError> {
        Ok(Frame::Null)
    }
}
