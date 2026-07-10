# 数据引擎架构图 (Eviction Module)

经过模块化拆分和“零成本抽象”重构后，目前底层数据引擎的架构如下：

```mermaid
classDiagram
    direction TB

    %% 对外接口层
    class Db {
        +store: Storage
    }
    class Storage {
        +store: Arc~Vec~Arc~MemoryCache~~~
        +lock_write(key): LockedDb
        +lock_read(key): LockedDb
    }

    %% 核心静态路由枚举 (Zero-Cost Abstraction)
    class LockedDb {
        <<Enum>>
        WriteNormal(DirectCacheNode)
        WriteLua(LuaCacheNode)
        ReadNormal(DirectCacheNode)
        ReadLua(LuaCacheNode)
        +insert(key, value)
        +select(key)
        +take(key)
        +delete(key)
        +commit()
    }

    %% 行为契约 (Traits)
    class KvOperator {
        <<Trait>>
        +insert(key, value)
        +select(key)
        +take(key)
        +delete(key)
    }
    class Transactional {
        <<Trait>>
        +commit()
    }
    class LockOwner {
        <<Trait>>
        +get_memory_usage()
        +add_memory(size)
        +sub_memory(size)
    }
    class EvictionPolicy {
        <<Trait>>
        +on_write(key)
        +on_read(key)
        +on_delete(key)
        +pop_victim()
    }

    %% 具体实现节点
    class DirectCacheNode {
        <<Enum>>
        Writeguard(OwnedRwLockWriteGuard)
        Readguard(OwnedRwLockReadGuard)
    }
    class LuaCacheNode {
        +db_store: DirectCacheNode
        +differ_map: HashMap~ChangeOp~
        +local_memory_diff: isize
    }

    %% 真正的存储底座
    class MemoryCache {
        +message: Vec~Arc~RwLock~MemoryCacheNode~~~
    }
    class MemoryCacheNode {
        +db_store: HashMap
        +approx_memory: AtomicUsize
        +evicition: Mutex~Box~EvictionPolicy~~
    }

    %% 淘汰策略实现
    class LruNode {
        -list: LruList
        -dict: HashMap
    }
    class LfuNode {
        -freq_map: ...
    }

    %% 依赖与实现关系
    Db --> Storage : 包含
    Storage --> MemoryCache : 管理 16 个 Db 实例
    MemoryCache --> MemoryCacheNode : 管理 64 个分片 (Shards)
    
    Storage ..> LockedDb : 产生
    LockedDb *-- DirectCacheNode : 包含
    LockedDb *-- LuaCacheNode : 包含
    LockedDb ..|> KvOperator
    LockedDb ..|> Transactional
    LockedDb ..|> LockOwner

    DirectCacheNode ..|> KvOperator
    DirectCacheNode ..|> LockOwner
    DirectCacheNode --> MemoryCacheNode : 持有读写锁

    LuaCacheNode ..|> KvOperator
    LuaCacheNode ..|> Transactional
    LuaCacheNode --> DirectCacheNode : 包装代理

    MemoryCacheNode *-- EvictionPolicy : 内部持有

    LruNode ..|> EvictionPolicy : 实现
    LfuNode ..|> EvictionPolicy : 实现 (待写)

```

## 架构分层说明

1. **入口层 (Entry Layer)**: `Db` -> `Storage` -> `MemoryCache`。负责定位 DB 索引和 Shard (分片) 索引，返回持有锁的保护器。
2. **零成本路由层 (Routing Layer)**: `LockedDb` (在 `src/db/mod.rs` 中)。它作为整个引擎的唯一对外操作句柄，通过静态 `match` 匹配，将外界对 `insert/select` 的调用，零开销地转发给底层的节点。
3. **接口契约层 (Trait Layer)**: `src/db/eviction/traits.rs`。定义了引擎的能力边界：`KvOperator` (读写)，`Transactional` (事务提交)，`LockOwner` (内存记账)。这些纯粹是行为约束，不再带有任何异步 (`async`) 负担。
4. **操作节点层 (Operator Layer)**:
   - `DirectCacheNode` (`src/db/eviction/direct_node.rs`): 最纯粹的直接操作器，持有真正的 `RwLockGuard`，它的读写会直接改变内存。
   - `LuaCacheNode` (`src/db/eviction/lua_node.rs`): 事务操作器，内部维护了一个 `differ_map` (悔棋小本子)，拦截所有的改动，直到 `commit` 被调用才会写回 `DirectCacheNode`。
5. **物理存储层 (Physical Layer)**: `MemoryCacheNode` (`src/db/eviction/cache_store.rs`)。它是真正的仓库，包含一个 `HashMap` (存数据)，一个 `AtomicUsize` (存自己分片的账本)，以及一把互斥锁保护的 `EvictionPolicy` (淘汰策略小弟)。
