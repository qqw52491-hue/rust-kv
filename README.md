# Rust-KV: 高性能企业级异步内存数据库 涉及内容

A Redis-compatible high-performance in-memory database written in Rust
rust
redis
redis-compatible
database
key-value
kv
in-memory-database
tokio
async
lua

**Rust-KV** 是一个从零构建的、兼容 Redis 协议 (RESP) 的高性能分布式内存数据库原型。

本项目不仅是一个 KV 存储，更是一个深度探索 **Rust 异步运行时 (Tokio)**、**无锁编程**、**Actor 模型** 以及 **FFI 高性能交互** 的工业级实践。它在单机环境下实现了超高并发与极低延迟，特别是在 Lua 脚本执行引擎上，通过创新的架构设计，实现了超越原生 Redis 的 ACID 保证。

## 🚀 极致性能 (Performance Benchmark)

基于 Linux 环境本地回环测试，使用 `redis-benchmark` 工具（100万纯随机 Key，Pipeline=32，连接数 50）与原生 Redis 进行了极限吞吐量对比测试：

| 测试场景 (100万随机Key) | 原生 Redis (6379) | Rust-KV (6380) | 核心优势与突破点 |
| :--- | :--- | :--- | :--- |
| **纯内存读取 (GET)** | 1,654,533 RPS (0.88ms) | **1,655,081 RPS** (0.87ms) | **多核无锁读并发！** 多线程结合 1024 个 Shard 分段读锁 (`RwLock::read`)，彻底释放多核并发能力。 |
| **纯内存写入 (SET)** | 773,874 RPS (1.87ms) | **1,093,135 RPS** (1.31ms) | **领先原生 Redis 超 31 万 RPS！** 64字节缓存行对齐 (`#[repr(align(64))]`) + 细粒度分片写锁，彻底打破单核写入物理瓶颈。 |
| **极限内存淘汰 (Eviction)** | 177,904 RPS (非 Pipeline) | **186,699 RPS** (非 Pipeline) | **极速无阻塞垃圾回收！** 在内存被打满 (80MB) 触发高频淘汰时，后台异步清理任务完美削峰，SET 性能死死咬住 18.6万 RPS 毫无衰减！ |
| **混合读写 (Mixed)** | **-** | **1,000,000+ RPS** | Actor 模型无锁调度，结合 4 线程并发解析，消除多路复用网络解析瓶颈。 |


> **复现指令参考:**
>
> ```bash
> memtier_benchmark -s 127.0.0.1 -p 6379 -t 4 -c 50 --pipeline=32 --ratio=1:1
> ```

## 🛠️ 核心架构与技术亮点 (Core Architecture)

### 1. 革命性的多线程 Lua 引擎 (Multi-Reactor Lua Engine)
这是本项目最核心的创新点，解决了 Redis 单线程脚本阻塞的痛点，同时保证了比 Redis 更强的数据一致性。

* **Actor 模型架构:** 启动 N 个（默认 8 个）独立的 OS 线程，每个线程独占一个 Lua 虚拟机 (`mlua::Lua`)。外部请求通过 `flume/mpsc` 通道分发，实现了计算与 I/O 的彻底分离。
* **ThreadLocal 零开销注入 (Zero-Overhead Injection):**
    * 拒绝每次请求重复创建 Lua 上下文。
    * 利用 Rust 的 `thread_local!`，在线程启动时一次性注册 `redis.call` 绑定。
    * 运行时仅通过 TLS 指针交换上下文 (`CURRENT_ENV`)，将 FFI 调用开销降至纳秒级，从而实现了 **11w+ QPS** 的惊人性能。
* **智能负载均衡 (Queue-Aware Dispatch):** 调度器实时监控每个 Worker 的队列深度 (`capacity`)，自动将任务分发给最空闲的线程，避免了 Round-Robin 导致的队头阻塞 (Head-of-Line Blocking) 问题。

### 2. 真·ACID 事务支持 (True ACID Transactions)
超越 Redis 的 "Scripting" 语义，实现了真正的数据库级事务。

* **写缓冲 (Write Buffering):** Lua 脚本执行期间，所有的写入操作 (`SET`, `DEL`) 不会直接修改底层数据，而是记录在 `LuaCacheNode` 的 `differ_map` 差异缓冲区中。
* **原子提交与回滚 (Commit or Rollback):**
    * **Success:** 只有脚本成功返回，差异数据才会原子性地应用到底层存储。
    * **Failure:** 如果脚本中途报错（Panic 或 Error），缓冲区直接丢弃，底层数据毫发无损。
* **对比:** 原生 Redis 脚本若中途失败，已执行的写操作无法撤销，破坏原子性。

### 3. 精细化分片锁架构 (Sharded Locking)
为了在多线程环境下最大化并发度，彻底摒弃全局大锁。

* **两级分片:** `16 Databases` × `64 Shards/DB` = **1024 个独立的锁域**。
* **无锁哈希:** 采用 `fxhash` 进行极速路由。
* **锁粒度控制:** 读写操作仅锁定 Key 所在的特定分片 (`RwLock`)，使得 99% 的并发请求完全无竞争。

### 4. 智能 AOF 持久化 (Smart Batching AOF)
解决了高并发写入下的磁盘 I/O 瓶颈。

* **背压与削峰:** AOF 通道 (`mpsc::channel`) 作为天然的缓冲区，吸收突发流量。
* **贪婪批处理 (Greedy Batching):** 后台落盘任务 (`aof_writer_task`) 采用“贪婪模式”：一旦唤醒，会尽可能多地从通道中拉取积压数据（比如一次 5000 条），合并为一次 `write_all` 和 `flush` 系统调用。这使得系统在 IOPS 有限的 SSD 上也能跑满带宽。

### 5. 健壮的工程化实现
* **优雅停机 (Graceful Shutdown):** 基于 `broadcast` 通道实现的双层停机（应用层 -> 基础设施层），确保在服务关闭前，所有挂起的 AOF 数据都被刷入磁盘，数据零丢失。
* **零拷贝协议解析:** 基于 `bytes::BytesMut` 和 `Cursor` 实现的 RESP 解析器，在解析过程中零内存分配。

### 6. 无阻塞异步垃圾回收 (Zero-Blocking GC)
在高频写入导致内存溢出时，传统的同步淘汰会严重阻塞网络请求，本项目实现了极速的异步淘汰引擎：
* **动态平滑调度:** 后台巡逻任务监控全局原子内存 (`GLOBAL_MEMORY`)。健康时休眠让出 CPU；溢出时 0 延时立刻启动清理。
* **微创切片清理 (Time-Sliced Eviction):** 将海量淘汰任务切分为 20ms 的时间片，每处理 200 个 Key 主动 `yield_now().await` 让出 Tokio 上下文，彻底杜绝 GC 任务引发的协程饥饿 (Starvation)。
* **安全高效的淘汰操作:** 通过向底层分片获取瞬时写锁 (`Writeguard`) 并复用统一的高内聚 `delete` 接口，原子性地完成 HashMap 删除、LRU 链表解绑和全局内存扣减。在 100 万级请求轰炸下依然保持内存与数据的绝对一致。
* **Unsafe 手写 O(1) LRU:** 为了追求极致的淘汰性能，使用 `NonNull` 裸指针手写双向链表，结合 `HashMap` 索引，实现了工业级的极速 LRU 算法。

### 7. 原生 JSON 存储与 JSON Pointer 解析
彻底告别传统 KV 存储中 JSON 需要序列化/反序列化带来的巨大性能与网络开销。
* **类型颠覆:** 在底层内存直接以 `serde_json::Value` 树形结构存储，将 JSON 提升为与 String/List 同等的一等公民。
* **原生 JSON Pointer 标准支持:** 严格遵循 IETF **RFC 6901** 标准，支持通过 `/address/city` 等路径规范直接访问内部叶子节点，彻底解决 `.` 分割路径带来的转义和解析歧义。
* **$O(D)$ 极速原地修改:** 引擎在收到长路径修改指令（如 `JSON.SET user:1 /age 26`）时，借助标准库 `pointer_mut` 的极致优化，仅需 $O(层级深度)$ 的微小开销即可在内存中实现精准原地替换。相比于传统 "GET -> 反序列化 -> 修改 -> 序列化 -> SET" 模式，网络 I/O 节省近 99%，并结合细粒度写锁彻底消灭并发更新覆盖 (Lost Update) 的死局。

## 📦 已支持的命令 (Supported Commands)

目前 Rust-KV 已经实现并支持了以下核心命令集：

### 1. 字符串 (String)
* `SET` / `GET`
* `MSET` / `MGET` (批量操作)

### 2. 列表 (List)
* `LPUSH` / `LPOP`
* **`BLPOP` (阻塞式弹出)**: 支持客户端在列表为空时安全阻塞等待，直到有新元素被 push 或超时，这是实现高性能消息队列的基础。

### 3. 哈希表 (Hash)
* `HSET` / `HGET` / `HDEL`

### 4. 有序集合 (Sorted Set / ZSet)
* `ZADD` / `ZSCORE` / `ZRANK` / `ZRANGE` / `ZREM`

### 5. 原生 JSON (Native JSON)
* **`JSON.SET`**: 支持按照 JSON Pointer 规范直接更新或创建内存中的 JSON 树节点。
* **`JSON.GET`**: 支持按照 JSON Pointer 规范提取局部 JSON 节点。
