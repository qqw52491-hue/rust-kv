# Rust-KV 架构说明文档

## 1. 项目概览

Rust-KV 是一个兼容 Redis 协议 (RESP) 的高性能异步内存数据库，基于 Tokio 异步运行时构建。

### 核心特性
- **RESP 协议兼容**：可使用 `redis-cli` 直接连接
- **多线程 Lua 引擎**：8 个独立 Worker 线程，智能负载均衡
- **真 ACID 事务**：Lua 脚本支持写缓冲 + 原子提交/回滚
- **精细化分片锁**：16 DB × 64 Shards = 1024 个独立锁域
- **智能 AOF 持久化**：贪婪批处理 + 优雅停机数据零丢失

---

## 2. 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              客户端层 (Client Layer)                        │
│                         redis-cli / 任意 Redis 客户端                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼ TCP 连接
┌─────────────────────────────────────────────────────────────────────────────┐
│                              网络层 (Network Layer)                         │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                         TcpListener                                  │   │
│  │                    监听 127.0.0.1:6379                               │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼ accept()                            │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                     handle_connection()                              │   │
│  │              每个连接独立异步任务 + TaskLocal 状态                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼ BytesMut 缓冲区
┌─────────────────────────────────────────────────────────────────────────────┐
│                           协议解析层 (Protocol Layer)                        │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                       parse_frame()                                  │   │
│  │            RESP 协议零拷贝解析 (Cursor + memchr SIMD)                 │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼ Frame 枚举                          │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    Command::try_from(Frame)                          │   │
│  │                 Frame → Command 结构化转换                            │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼ Command 枚举
┌─────────────────────────────────────────────────────────────────────────────┐
│                          命令执行层 (Command Layer)                          │
│                                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │  GET 命令   │  │  SET 命令   │  │  PING 命令  │  │  EVAL 命令  │        │
│  │  (读锁)     │  │  (写锁)     │  │  (无锁)     │  │  (Lua引擎)  │        │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
│                                      │                                      │
│                                      ▼                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    execute_command_normal()                          │   │
│  │              获取分片锁 → 执行命令 → 写入 AOF                         │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            存储层 (Storage Layer)                           │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                          Db (顶层结构)                                │   │
│  │                    16 个 Database 实例                                │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                       MemoryCache (单库)                             │   │
│  │                   64 个 RwLock<MemoryCacheNode>                      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                      │                                      │
│                                      ▼                                      │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                    MemoryCacheNode (单分片)                          │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐      │   │
│  │  │   HashMap<K,V>  │  │  EvictionPolicy │  │  AtomicUsize    │      │   │
│  │  │   (数据存储)     │  │  (LRU/LFU淘汰)  │  │  (内存记账)     │      │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                 ▼
┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│    AOF 持久化模块     │  │    Lua 引擎模块       │  │    淘汰策略模块       │
│                      │  │                      │  │                      │
│  • 贪婪批处理写入     │  │  • 8 Worker 线程     │  │  • LRU (已实现)      │
│  • 背压削峰          │  │  • 智能负载均衡       │  │  • LFU (待实现)      │
│  • 优雅停机数据保护   │  │  • 写缓冲 + ACID     │  │  • 内存阈值淘汰      │
└──────────────────────┘  └──────────────────────┘  └──────────────────────┘
```

---

## 3. 核心模块详解

### 3.1 入口与编排 (`lib.rs`)

```
run()
  ├── 初始化 AOF 通道 (mpsc::channel, 容量 1,000,000)
  ├── 初始化停机广播 (broadcast::channel)
  ├── 初始化 Lua VM 池 (flume::bounded, 容量 50)
  ├── 启动 Lua Actor 系统 (8 Worker, 队列 100,000)
  ├── 启动 AOF 写入任务
  ├── 启动时间缓存任务 (每 10ms 更新)
  ├── 启动 TTL 淘汰任务
  ├── 启动内存淘汰任务
  ├── 恢复 AOF 数据
  └── 进入连接接受循环
```

**关键设计**：
- 使用 `tokio::select!` 同时监听新连接和停机信号
- 每个连接 spawn 独立任务，通过 `TaskLocal` 管理连接状态
- 连接句柄统一收集到 `Vec<JoinHandle>`，停机时等待所有连接完成

---

### 3.2 网络与协议层

#### 3.2.1 连接处理 (`server.rs`)

```rust
handle_connection(socket, db, connection_content)
  ├── 创建 BytesMut 缓冲区 (1024 字节)
  ├── 循环: tokio::select! { socket.read_buf(), shutdown_signal }
  │   ├── GotData → explain_execute_command()
  │   ├── Shutdown → break
  │   └── ClientClosed → break
  └── 返回结果通过 socket.write_all() 发送
```

**错误处理策略**：
- 协议解析错误：跳过当前命令，寻找下一个 `*` 开头的 RESP 包
- 命令执行错误：返回 `Frame::Error` 给客户端，继续处理

#### 3.2.2 RESP 解析器 (`core_explain.rs`)

```
parse_frame(buf: &[u8])
  └── parse_frame_from_cursor(cursor)
      ├── b'*' → parse_array_from_cursor()    # 递归解析数组
      ├── b'$' → parse_bulk_string_from_cursor() # 解析批量字符串
      └── 其他 → ProtocolError
```

**性能优化**：
- 使用 `Cursor<&[u8]>` 进行只读预演，成功后才消耗缓冲区
- 使用 `memchr::memmem::Finder` SIMD 指令加速 CRLF 查找
- 解析过程零内存分配（除最终的 `Bytes::copy_from_slice`）

---

### 3.3 存储层

#### 3.3.1 数据库结构

```
Db
└── Storage
    └── Arc<Vec<Arc<MemoryCache>>>  # 16 个 Database
        └── MemoryCache
            └── Vec<Arc<RwLock<MemoryCacheNode>>>  # 64 个分片
                └── MemoryCacheNode
                    ├── db_store: HashMap<Arc<String>, ValueEntry>
                    ├── approx_memory: AtomicUsize
                    └── evicition: Mutex<Box<dyn EvictionPolicy>>
```

#### 3.3.2 分片路由

```rust
fn get_shard_index<K: Hash>(key: &K) -> usize {
    let mut hasher = FxHasher::default();
    key.hash(&mut hasher);
    (hasher.finish() as usize) % 64  // 64 个分片
}
```

使用 `fxhash` 进行极速哈希路由，单次哈希计算约 3-5ns。

#### 3.3.3 锁机制

| 操作 | 锁类型 | 粒度 |
|------|--------|------|
| GET | `OwnedRwLockReadGuard` | 单分片 |
| SET | `OwnedRwLockWriteGuard` | 单分片 |
| EVAL (Lua) | `OwnedRwLockWriteGuard` | 按 key 排序锁定，避免死锁 |

**并发度**：16 DB × 64 Shards = 1024 个独立锁域，99% 的请求完全无竞争。

---

### 3.4 命令执行层

#### 3.4.1 命令生命周期

```
Frame (协议层)
    │
    ▼ TryFrom
Command (结构化)
    │
    ▼ execute_command_normal()
    ├── get_command_lock()      # 获取分片锁
    ├── execute_command_hook()  # 执行命令
    └── exe_aof_command()       # 写入 AOF
    │
    ▼
Frame (响应)
```

#### 3.4.2 命令类型

| 命令 | 结构体 | 锁类型 | AOF |
|------|--------|--------|-----|
| GET | `GetCommand` | 读锁 | 否 |
| SET | `SetCommand` | 写锁 | 是 |
| PING | `PingCommand` | 无 | 否 |
| EVAL | `EvalCommand` | Lua 管理 | 是 |
| 其他 | `UnimplementCommand` | 无 | 否 |

---

### 3.5 Lua 引擎模块

#### 3.5.1 Actor 模型架构

```
┌─────────────────────────────────────────────────────────────────┐
│                        LuaRouter (调度器)                       │
│                    智能负载均衡: 选择队列最空的 Worker             │
└─────────────────────────────────────────────────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│   Worker #0      │ │   Worker #1      │ │   Worker #N      │
│   (OS 线程)      │ │   (OS 线程)      │ │   (OS 线程)      │
│                  │ │                  │ │                  │
│  ┌────────────┐  │ │  ┌────────────┐  │ │  ┌────────────┐  │
│  │ Lua VM     │  │ │  │ Lua VM     │  │ │  │ Lua VM     │  │
│  │ (独立实例)  │  │ │  │ (独立实例)  │  │ │  │ (独立实例)  │  │
│  └────────────┘  │ │  └────────────┘  │ │  └────────────┘  │
│                  │ │                  │ │                  │
│  thread_local!   │ │  thread_local!   │ │  thread_local!   │
│  CURRENT_ENV     │ │  CURRENT_ENV     │ │  CURRENT_ENV     │
└──────────────────┘ └──────────────────┘ └──────────────────┘
          ▲                   ▲                   ▲
          │                   │                   │
          └───────────────────┴───────────────────┘
                              │
                    mpsc::channel (队列)
```

#### 3.5.2 负载均衡算法

```rust
pub async fn dispatch(&self, task: LuaTask) -> Result<(), KvError> {
    let mut best_index = 0;
    let mut max_capacity = 0;

    for (i, sender) in self.senders.iter().enumerate() {
        let cap = sender.capacity();
        if cap > max_capacity {
            max_capacity = cap;
            best_index = i;
        }
    }

    self.senders[best_index].send(task).await
}
```

**策略**：Least Queue Depth (最短队列优先)，避免 Round-Robin 导致的队头阻塞。

#### 3.5.3 ACID 事务实现

```
Lua 脚本执行流程:
  1. init_lua_pre()
     ├── 解析 KEYS，计算涉及的分片
     ├── 按序获取分片锁 (避免死锁)
     └── 创建 LuaCacheNode (写缓冲层)

  2. redis.call("SET", key, value)
     ├── 写入 differ_map (不修改底层数据)
     └── 更新 local_memory_diff

  3. 脚本执行完成
     ├── 成功 → commit() 将 differ_map 应用到底层
     └── 失败 → 丢弃 differ_map，底层数据不变
```

**对比 Redis**：Redis 脚本中途失败时，已执行的写操作无法撤销；Rust-KV 实现了真正的原子性。

---

### 3.6 AOF 持久化模块

#### 3.6.1 写入流程

```
命令执行完成
    │
    ▼ aof_tx.send(serialized_command)
┌─────────────────────────────────────────────────────────────────┐
│                    AOF Writer Task                              │
│                                                                 │
│  loop {                                                         │
│      // 1. 等待第一条消息或停机信号                               │
│      let first_msg = tokio::select! { rx.recv(), shutdown }     │
│                                                                 │
│      // 2. 贪婪批处理: 尽可能多地拉取积压消息                      │
│      buffer.push(first_msg)                                     │
│      while buffer.len() < 5000 {                                │
│          match rx.try_recv() {                                  │
│              Ok(msg) => buffer.push(msg),                       │
│              Err(_) => break,                                   │
│          }                                                      │
│      }                                                          │
│                                                                 │
│      // 3. 批量写入磁盘                                          │
│      for msg in &buffer {                                       │
│          file.write_all(msg).await                              │
│      }                                                          │
│      file.flush().await                                         │
│      buffer.clear()                                             │
│  }                                                              │
│                                                                 │
│  // 4. 停机收尾: 排空通道，确保数据零丢失                          │
│  while let Ok(msg) = rx.try_recv() { ... }                      │
└─────────────────────────────────────────────────────────────────┘
```

#### 3.6.2 数据恢复

```
explain_execute_aofcommand(path, db)
  ├── 分配 512MB 读取缓冲区
  ├── 循环读取文件
  │   ├── parse_frame() 解析 RESP
  │   ├── Command::try_from() 转换命令
  │   └── execute_command() 执行命令
  └── 处理跨块数据 (copy_within)
```

---

### 3.7 淘汰策略模块

#### 3.7.1 接口定义

```rust
pub trait EvictionPolicy: Send + Sync {
    fn on_write(&mut self, key: Arc<String>);
    fn on_read(&mut self, key: &Arc<String>);
    fn on_delete(&mut self, key: Arc<String>);
    fn get_random_sample_key(&self) -> Option<Arc<String>>;
    fn pop_victim(&mut self) -> Option<Arc<String>>;
}
```

#### 3.7.2 LRU 实现

使用 `NonNull` 裸指针手写双向链表 + `HashMap` 索引，实现 O(1) 的淘汰操作。

#### 3.7.3 淘汰触发

| 触发方式 | 实现 |
|----------|------|
| TTL 过期 | 后台定时任务扫描 + 读取时惰性删除 |
| 内存超限 | 后台监控任务，超过阈值时批量淘汰 |

---

### 3.8 停机模块

#### 3.8.1 双层停机机制

```
┌─────────────────────────────────────────────────────────────────┐
│                     停机信号传播流程                             │
└─────────────────────────────────────────────────────────────────┘

  SIGINT / SIGTERM
         │
         ▼
  shutdown_listener()
         │
         ▼ app_shutdown_tx.send(())
  ┌──────┴──────┐
  │             │
  ▼             ▼
连接层停止    AOF 层停止
  │             │
  │ 等待所有    │ 排空通道
  │ 连接完成    │ 数据落盘
  │             │
  ▼             ▼
  └──────┬──────┘
         │
         ▼ infra_shutdown_tx.send(())
  ┌──────┴──────┐
  │             │
  ▼             ▼
TTL 任务停止  时间任务停止
内存任务停止
```

**设计原则**：
- 应用层先停（不再接受新请求）
- 基础设施层后停（确保数据完全落盘）

---

### 3.9 时间缓存模块

```rust
pub static CACHED_TIME_MS: AtomicU64 = AtomicU64::new(0);

pub async fn start_time_caching_task(sender: Sender<()>) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(10)) => {
                CACHED_TIME_MS.store(system_time_ms(), Ordering::Relaxed);
            }
            _ = receiver.recv() => break,
        }
    }
}

pub fn get_cached_time_ms() -> u64 {
    CACHED_TIME_MS.load(Ordering::Relaxed)
}
```

**优化点**：避免每次获取时间戳都调用系统调用，每 10ms 更新一次全局缓存。

---

## 4. 数据流图

### 4.1 普通命令流程

```
客户端发送 SET foo bar
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. TcpStream.read_buf() → BytesMut                             │
│ 2. parse_frame() → Frame::Array(["SET", "foo", "bar"])         │
│ 3. Command::try_from() → Command::Set(SetCommand{...})         │
│ 4. get_command_lock() → lock_write("foo") → 分片 42 写锁       │
│ 5. execute_command_hook() → 插入 HashMap                       │
│ 6. exe_aof_command() → aof_tx.send(serialized)                 │
│ 7. Frame::Simple("OK") → socket.write_all()                   │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Lua 脚本流程

```
客户端发送 EVAL "redis.call('SET', KEYS[1], ARGV[1])" 1 foo bar
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│ 1. 解析为 EvalCommand{script, keys:["foo"], args:["bar"]}      │
│ 2. LuaRouter.dispatch() → 选择队列最空的 Worker                 │
│ 3. Worker 线程:                                                 │
│    a. init_lua_pre()                                            │
│       - 计算 key "foo" 的分片索引                                │
│       - 按序获取分片锁                                          │
│       - 创建 LuaCacheNode (写缓冲)                              │
│       - 注入 KEYS/ARGV 到 Lua 全局变量                          │
│    b. lua.load(script).eval_async()                             │
│       - redis.call("SET", "foo", "bar")                        │
│       - 写入 differ_map，不修改底层                              │
│    c. commit() → 将 differ_map 应用到 HashMap                  │
│ 4. 返回结果给客户端                                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## 5. 关键设计模式

### 5.1 TaskLocal 状态管理

```rust
task_local! {
    pub static CONN_STATE: ConnectionState;
}

// 使用方式
CONN_STATE.scope(initial_state, async {
    // 在这个作用域内，任何地方都可以通过 CONN_STATE.with() 访问连接状态
    let db_index = CONN_STATE.with(|state| state.selected_db);
}).await;
```

**优势**：避免在所有函数调用中传递 `selected_db` 和 `client_address` 参数。

### 5.2 策略模式 (EvictionPolicy)

```rust
pub trait EvictionPolicy: Send + Sync {
    fn on_write(&mut self, key: Arc<String>);
    fn on_read(&mut self, key: &Arc<String>);
    // ...
}

// 运行时选择
let policy: Box<dyn EvictionPolicy> = match config_type {
    EvictionType::LRU => Box::new(LruNode::new()),
    EvictionType::LFU => Box::new(LfuNode::new()),
};
```

### 5.3 事务缓冲模式 (LuaCacheNode)

```rust
pub struct LuaCacheNode {
    db_store: DirectCacheNode,           // 底层真实存储
    differ_map: HashMap<Arc<String>, ChangeOp>,  // 变更缓冲
    local_memory_diff: isize,            // 内存差异记账
}

// 写操作: 先写缓冲
async fn insert(&mut self, key: Arc<String>, value: ValueEntry) {
    self.differ_map.insert(key, ChangeOp::Update(value));
}

// 提交: 应用到底层
async fn commit(&mut self) {
    for (key, change) in self.differ_map.drain() {
        match change {
            ChangeOp::Update(v) => self.db_store.insert(key, v).await,
            ChangeOp::Delete => self.db_store.delete(&key).await,
        }
    }
}
```

---

## 6. 性能优化总结

| 优化点 | 技术手段 | 效果 |
|--------|----------|------|
| 协议解析 | `Cursor` + `memchr` SIMD | 零拷贝，CRLF 查找加速 4-8 倍 |
| 哈希路由 | `fxhash` | 比标准库 HashMap 快 2-3 倍 |
| 锁粒度 | 1024 分片锁 | 99% 请求无竞争 |
| 时间获取 | `AtomicU64` 缓存 | 避免系统调用，10ms 更新 |
| AOF 写入 | 贪婪批处理 | 单次 write_all，减少系统调用 |
| Lua 引擎 | ThreadLocal + Actor | 纳秒级 FFI 调用开销 |
| 内存记账 | `AtomicUsize` | 无锁内存统计 |

---

## 7. 模块依赖关系

```
main.rs
  └── lib.rs (run)
        ├── config.rs          # 配置定义
        ├── context.rs         # 连接状态 (TaskLocal)
        ├── server.rs          # 网络连接处理
        ├── core_explain.rs    # RESP 协议解析
        ├── core_exchange.rs   # Frame → Command 转换
        ├── core_execute.rs    # 命令执行调度
        ├── core_aof.rs        # AOF 持久化
        ├── core_time.rs       # 时间缓存
        ├── shutdown.rs        # 优雅停机
        ├── error.rs           # 错误类型定义
        ├── types.rs           # 数据类型 (Value, ValueEntry)
        ├── db/                # 存储层
        │   ├── mod.rs         # Db, Storage 结构
        │   ├── string.rs      # String 类型操作
        │   ├── hash.rs        # Hash 类型操作
        │   ├── list.rs        # List 类型操作
        │   └── eviction/      # 淘汰策略
        │       ├── mod.rs     # MemoryCache, 分片逻辑
        │       ├── lru.rs     # LRU 实现
        │       └── lfu.rs     # LFU 实现 (待完成)
        ├── command_exchange/  # Frame → Command 转换
        │   ├── mod.rs         # CommandExchange trait
        │   ├── string.rs      # SET/GET 命令解析
        │   └── common.rs      # PING 等通用命令
        ├── command_execute/   # Command 执行逻辑
        │   ├── mod.rs         # CommandExecutor trait
        │   ├── string.rs      # SET/GET 执行
        │   └── common.rs      # PING 等通用执行
        ├── aof_exchange/      # Command → AOF 序列化
        │   ├── mod.rs         # AofContent 定义
        │   └── string.rs      # SET 命令序列化
        └── lua/               # Lua 引擎
            ├── lua_vm.rs      # Lua VM 初始化
            ├── lua_work.rs    # Actor 系统
            └── lua_exchange.rs # Lua 类型转换
```

---

## 8. 配置项

| 配置 | 当前值 | 位置 |
|------|--------|------|
| 监听地址 | `127.0.0.1:6379` | `lib.rs:63` |
| Database 数量 | 16 | `db/mod.rs:57` |
| 分片数量 | 64 | `eviction/mod.rs:19` |
| Lua Worker 数量 | 8 | `lib.rs:53` |
| Lua 队列大小 | 100,000 | `lib.rs:53` |
| AOF 通道容量 | 1,000,000 | `lib.rs:39` |
| 淘汰策略 | LRU | `config.rs:17` |
| 时间缓存间隔 | 10ms | `core_time.rs:26` |
| 内存淘汰阈值 | 8MB | `lib.rs:100` |

---

## 9. 待优化项

1. **配置外部化**：将硬编码配置提取到 TOML/YAML 配置文件
2. **LFU 淘汰算法**：当前仅有 LRU，LFU 标记为 `todo!()`
3. **更多 Redis 命令**：HSET、LPUSH、INCR、EXPIRE 等
4. **Cluster 支持**：实现 Redis Cluster 协议
5. **监控指标**：集成 Prometheus 指标导出
6. **单元测试**：补充各模块的测试覆盖
