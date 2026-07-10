# Rust 多态分发与异步模式速查表

> 写给未来的自己：别再被编译器逼着加 `#[async_trait]` 了！

---

## 一、多态分发方式对比

| 方式 | 语法 | 运行时开销 | 适用场景 |
|------|------|-----------|---------|
| **Enum 静态分发** | `enum Foo { A(TypeA), B(TypeB) }` | ✅ **零开销**，编译期内联 | 变体数量固定且已知 |
| **泛型静态分发** | `fn do_it<T: Trait>(t: T)` | ✅ **零开销**，单态化 | 调用方知道具体类型 |
| **Trait Object 动态分发** | `Box<dyn Trait>` | ❌ 堆分配 + 虚表跳转 | 变体数量不固定（插件系统） |

### 选择原则

```
你知道所有的具体类型吗？
    ├── 是 → 用 Enum（首选）或泛型
    └── 否（比如用户可以自定义插件）→ 用 Box<dyn Trait>
```

### 本项目的实际例子

```rust
// ✅ 我们的做法：Enum 静态分发，零开销
pub enum LockedDb {
    WriteNormal(DirectCacheNode),
    WriteLua(LuaCacheNode),
    ReadNormal(DirectCacheNode),
    ReadLua(LuaCacheNode),
}

// ❌ 之前的做法：动态分发，每次调用都要查虚表
pub enum LockedDb {
    Write(Box<dyn KvOperator>),
    Read(Box<dyn KvOperator>),
}
```

---

## 二、异步与 Trait 的组合关系

| 组合 | 是否需要 `#[async_trait]` | 开销 | 说明 |
|------|:------------------------:|------|------|
| 同步方法 + 不用 `dyn` | ❌ | 零 | 最理想 |
| 同步方法 + `Box<dyn>` | ❌ | 虚表跳转 | 可以接受 |
| **异步方法 + 不用 `dyn`** | **❌** | **零** | **用 `impl Future` 返回（RPITIT）** |
| 异步方法 + `Box<dyn>` | **✅ 必须** | 虚表 + 堆分配 Future | **尽量避免！** |

### 关键规则

> **只有 `dyn` + `async` 同时出现时，才被迫用 `#[async_trait]`。**
> 
> 能用 Enum 或泛型替代 `dyn` 的地方，就永远不需要它。

---

## 三、哪些操作真正需要 `async`？

### ✅ 需要 async（有真正的 I/O 等待）

| 操作 | 原因 |
|------|------|
| 获取 `RwLock` / `Mutex` | 可能需要等待其他任务释放锁 |
| 网络 I/O（TCP 读写） | 等待数据到达 |
| 磁盘 I/O（读写文件） | 等待磁盘完成 |
| Channel 发送/接收 | 等待对端就绪 |
| `tokio::time::sleep` | 等待定时器 |

### ❌ 不需要 async（纯 CPU 同步操作）

| 操作 | 原因 |
|------|------|
| `HashMap::insert / get / remove` | 纯内存计算，纳秒级 |
| `AtomicUsize::fetch_add` | 一条 CPU 指令 |
| `Vec::push / pop` | 纯内存 |
| 数学计算、哈希计算 | 纯 CPU |
| `std::sync::Mutex::lock` | 极短临界区，不该用 async |

### 判断口诀

> **拿到锁之前 → async（等别人放锁）**
> 
> **拿到锁之后 → sync（你已经是老大了，直接操作内存）**

---

## 四、`#[async_trait]` 的真相

当你写：
```rust
#[async_trait]
trait MyTrait {
    async fn do_something(&self);
}
```

宏会展开成：
```rust
trait MyTrait {
    fn do_something(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;
    //                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    //                        每次调用都在堆上分配！
}
```

**代价：**
- 每次方法调用 → 一次 `Box::new()`（堆内存分配，~10-50ns）
- 每次方法调用 → 一次虚表查找（间接跳转，~5ns）
- 阻止编译器内联优化

**在 QPS 百万级的数据库内核中，这些纳秒会累积成可观的开销。**

---

## 五、Rust 1.75+ 的 RPITIT（不用 `#[async_trait]` 的正确写法）

```rust
// 方法一：直接写 async fn（Rust 1.75+，但不能用于 dyn）
trait MyTrait {
    async fn do_something(&self);
}

// 方法二：手动返回 impl Future（更灵活，本项目 CommandExecutor 的写法）
trait CommandExecutor {
    fn execute(&self, ctx: CommandContext) 
        -> impl std::future::Future<Output = Result<Frame, KvError>> + Send;
}
```

两种写法都是 **零开销**，编译器在编译期就知道 Future 的具体类型和大小。

---

## 六、本项目架构决策记录

| 决策 | 选择 | 原因 |
|------|------|------|
| `LockedDb` 的多态 | Enum 静态分发 | 变体固定（Normal/Lua × Read/Write），零开销 |
| `KvOperator` 方法 | 同步（非 async） | 拿到锁后只操作 HashMap，无 I/O |
| `CommandExecutor` | `impl Future`（RPITIT） | 需要 async（获取锁 + AOF），但不用 dyn |
| `EvictionPolicy` | `Box<dyn>` | 策略可能扩展（LRU/LFU/随机），且在 `Mutex` 内部，开销可忽略 |

---

*最后更新：2026-07-09 — 零成本抽象重构完成后*
