# Rust-KV 开发者指南：如何添加新数据结构与命令

为了在 **Rust-KV** 中引入新的数据结构（例如 **List 列表** 结构，以及对应的 `LPUSH` / `LPOP` 命令），你需要遵循项目现有的分层设计。

本指南以添加 `LPUSH` 和 `LPOP` 命令为例，详细说明开发步骤。

---

## 整体开发流图
```
                        [ RESP 客户端 ]
                              │
                              ▼ (RESP 字节流)
                     [ core_explain.rs ] (帧解析)
                              │
                              ▼ (Frame::Array)
                     [ core_exchange.rs ] (命令路由)
                              │
                              ▼ (Command::LPush)
                     [ core_execute.rs ] (分片加锁与执行挂钩)
                              │
                              ▼
               [ command_execute/list.rs ] (命令执行核心)
               ┌──────────────┴──────────────┐
               ▼                             ▼
       (内存状态修改)                  (序列化为 AOF)
     [ db/eviction/ ]              [ aof_exchange/ ]
```

---

## 详细步骤

### 第一步：在内核数据中定义数据结构表示 (`src/types.rs`)

`Value` 枚举定义了存储引擎支持的所有底层数据类型。
1. **确认或添加 `Value` 变体**：
   项目目前已包含 `List` 变体，使用 `VecDeque<Element>` 承载。如果要增加全新的非 Simple 变体，需要在此处添加：
   ```rust
   #[derive(Clone, Debug)]
   pub enum Value {
       Simple(Element),
       List(VecDeque<Element>),        // 已存在
       Hash(HashMap<String, Element>), // 已存在
       Set(HashSet<Element>),          // 已存在
       // ZSet(SkipList...),           // 如果需要新增 ZSet，在此处声明
   }
   ```
2. **在 `Value::heap_memory_size()` 中实现堆大小计算**：
   项目的 LRU 驱逐策略依赖精确的内存统计。对于 `List` 等复合结构，需要遍历计算内部每个元素的堆大小，加上容器的堆空间占用：
   ```rust
   Value::List(deque) => {
       let elements_heap: usize = deque.iter().map(|e| e.heap_size()).sum();
       let container_heap = deque.capacity() * std::mem::size_of::<Element>();
       elements_heap + container_heap
   }
   ```

---

### 第二步：定义命令结构体与变体 (`src/error.rs`)

1. **在 `src/error.rs` 中定义对应的命令实体结构体**：
   ```rust
   #[derive(Debug, Clone)]
   pub struct LPushCommand {
       pub key: Arc<String>,
       pub values: Vec<Bytes>,
   }

   #[derive(Debug, Clone)]
   pub struct LPopCommand {
       pub key: Arc<String>,
   }
   ```
2. **将结构体加入顶层 `Command` 枚举**：
   ```rust
   #[derive(Debug, Clone)]
   pub enum Command {
       Set(SetCommand),
       Get(GetCommand),
       Ping(PingCommand),
       Unimplement(UnimplementCommand),
       EvalCommand(EvalCommand),
       // 新增的列表命令变体：
       LPush(LPushCommand),
       LPop(LPopCommand),
   }
   ```
3. **在 `Command::get_key()` 中匹配新命令，返回路由键**：
   这是分片锁分配锁域的核心，凡是带 key 的命令必须在此返回该 key：
   ```rust
   impl Command {
       pub fn get_key(&self) -> Option<&Arc<String>> {
           match self {
               Command::Set(cmd) => Some(&cmd.key),
               Command::Get(cmd) => Some(&cmd.key),
               Command::LPush(cmd) => Some(&cmd.key),
               Command::LPop(cmd) => Some(&cmd.key),
               _ => None,
           }
       }
   }
   ```

---

### 第三步：协议解析层转换 (`src/command_exchange/` & `src/core_exchange.rs`)

1. **在 `src/core_exchange.rs` 中匹配命令字**：
   ```rust
   "LPUSH" => {
       if length < 3 {
           return Err(ProtocolError("LPUSH 命令需要至少 2 个参数".into()));
       }
       LPushCommand::exchange(iter, command_name)
   }
   "LPOP" => {
       if length != 2 {
           return Err(ProtocolError("LPOP 命令仅需 1 个参数".into()));
       }
       LPopCommand::exchange(iter, command_name)
   }
   ```
2. **在 `src/command_exchange/` 目录下创建新文件（例如 `list.rs`），并实现 `CommandExchange` 特征**：
   ```rust
   use std::vec::IntoIter;
   use bytes::Bytes;
   use std::sync::Arc;
   use crate::error::{Command, Frame, KvError, LPushCommand, LPopCommand};
   use crate::command_exchange::{CommandExchange, extract_bulk_string, extract_bulk_bytes};

   impl CommandExchange for LPushCommand {
       fn exchange(mut itor: IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
           let key = Arc::new(extract_bulk_string(itor.next())?);
           let mut values = Vec::new();
           for frame in itor {
               values.push(extract_bulk_bytes(Some(frame))?);
           }
           Ok(Command::LPush(LPushCommand { key, values }))
       }
   }

   impl CommandExchange for LPopCommand {
       fn exchange(mut itor: IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
           let key = Arc::new(extract_bulk_string(itor.next())?);
           Ok(Command::LPop(LPopCommand { key }))
       }
   }
   ```

---

### 第四步：编写核心执行器 (`src/command_execute/` & `src/core_execute.rs`)

1. **在 `src/command_execute/` 目录下创建新文件（例如 `list.rs`），实现 `CommandExecutor` 接口**：
   ```rust
   use std::collections::VecDeque;
   use crate::{
       command_execute::{CommandContext, CommandExecutor},
       db::LockedDb,
       error::{Frame, KvError, LPushCommand, LPopCommand},
       types::{Element, Value, ValueEntry},
   };

   impl CommandExecutor for LPushCommand {
       async fn execute(&self, _ctx: CommandContext, db_lock: Option<&mut LockedDb>) -> Result<Frame, KvError> {
           if let Some(LockedDb::Write(map)) = db_lock {
               // 1. 查询现有值
               let mut list = match map.select(&self.key).await {
                   Some(entry) => match &entry.data {
                       Value::List(deque) => deque.clone(),
                       _ => return Ok(Frame::Error("WRONGTYPE 针对持有错误类型的 key 进行操作".into())),
                   },
                   None => VecDeque::new(),
               };

               // 2. 执行 LPUSH 写入
               for val in &self.values {
                   list.push_front(Element::String(val.clone()));
               }

               let new_len = list.len() as i64;
               
               // 3. 构建新的 ValueEntry 存回数据库（会自动触发内存统计更新）
               map.insert(self.key.clone(), ValueEntry::new(Value::List(list), None)).await;

               Ok(Frame::Integer(new_len))
           } else {
               Err(KvError::ProtocolError("LPUSH 缺少写锁守卫".into()))
           }
       }
   }

   impl CommandExecutor for LPopCommand {
       async fn execute(&self, _ctx: CommandContext, db_lock: Option<&mut LockedDb>) -> Result<Frame, KvError> {
           if let Some(LockedDb::Write(map)) = db_lock {
               // 1. 查询现有值
               let mut list = match map.select(&self.key).await {
                   Some(entry) => match &entry.data {
                       Value::List(deque) => deque.clone(),
                       _ => return Ok(Frame::Error("WRONGTYPE 针对持有错误类型的 key 进行操作".into())),
                   },
                   None => return Ok(Frame::Null),
               };

               // 2. 执行 LPOP
               match list.pop_front() {
                   Some(element) => {
                       let resp = match element {
                           Element::String(bytes) => Frame::Bulk(bytes),
                           Element::Int(i) => {
                               // 快速转换整数到 bytes
                               let bytes = crate::command_execute::parse_int_from_bytes(i);
                               Frame::Bulk(bytes)
                           }
                       };
                       // 3. 若列表空了则删除 Key，否则更新
                       if list.is_empty() {
                           map.delete(&self.key).await;
                       } else {
                           map.insert(self.key.clone(), ValueEntry::new(Value::List(list), None)).await;
                       }
                       Ok(resp)
                   }
                   None => Ok(Frame::Null),
               }
           } else {
               Err(KvError::ProtocolError("LPOP 缺少写锁守卫".into()))
           }
       }
   }
   ```
2. **在 `src/core_execute.rs` 中路由新命令**：
   * 在 `execute_command_hook()` 中，匹配 `Command::LPush` 和 `Command::LPop`，分别调用其 `execute`。
   * 在 `get_command_lock()` 中，为这两个命令指定需要的锁：
     ```rust
     Command::LPush(cmd) => db.store.lock_write(&cmd.key).await.into(),
     Command::LPop(cmd) => db.store.lock_write(&cmd.key).await.into(),
     ```

---

### 第五步：持久化支持 (`src/aof_exchange/`)

所有改变状态的命令都需要持久化到 AOF，以便重启时还原。
1. **在 `src/aof_exchange/` 目录下创建新文件（例如 `list.rs`），为命令实现 `CommandAofExchange` 接口**：
   将命令参数序列化为标准 RESP 协议帧：
   ```rust
   use crate::aof_exchange::{AofContent, CommandAofExchange};
   use crate::error::{LPushCommand, LPopCommand};

   impl CommandAofExchange for LPushCommand {
       async fn execute_aof<'a>(&self, ctx: AofContent<'a>) {
           let mut buf = Vec::new();
           // * (1 + 1 + values.len())
           let total_parts = 2 + self.values.len();
           buf.extend_from_slice(format!("*{}\r\n$5\r\nLPUSH\r\n", total_parts).as_bytes());
           buf.extend_from_slice(format!("${}\r\n", self.key.len()).as_bytes());
           buf.extend_from_slice(self.key.as_bytes());
           buf.extend_from_slice(b"\r\n");
           for val in &self.values {
               buf.extend_from_slice(format!("${}\r\n", val.len()).as_bytes());
               buf.extend_from_slice(val);
               buf.extend_from_slice(b"\r\n");
           }
           let _ = ctx.aof_tx.send(buf).await;
       }
   }
   // LPOP 类似，由于 LPOP 也会修改数据状态，所以同样需要序列化写入 AOF
   ```
2. **在 `src/aof_exchange/mod.rs` 中进行路由分发**：
   ```rust
   impl Command {
       pub async fn exe_aof_command<'a>(&self, ctx: AofContent<'a>) {
           match self {
               Command::Set(set_command) => set_command.execute_aof(ctx).await,
               Command::LPush(lpush_command) => lpush_command.execute_aof(ctx).await,
               Command::LPop(lpop_command) => lpop_command.execute_aof(ctx).await,
               _ => {}
           }
       }
   }
   ```

---

## 优势与设计精髓

1. **自动兼容多线程 Lua 引擎**：
   因为你在 `general_lua()` 中对 `redis.call` 进行了底层 `execute_command_hook(...)` 统一调用，因此只需完成上述核心步骤，**所有的 Lua 脚本都会自动支持新命令**！你可以直接在 Lua 中运行 `redis.call('LPUSH', 'list', 'v')`。
2. **自动集成 ACID 事务隔离与原子性**：
   因为 Lua 中调用的 `insert`/`delete`/`select` 是基于 `LuaCacheNode` 代理的，所有的临时写动作都会暂存在 `differ_map` 中。在脚本出错时依然可以自动 Rollback 事务，无需对新数据结构做任何特殊的事务编码。
