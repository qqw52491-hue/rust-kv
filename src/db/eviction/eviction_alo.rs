use std::{cmp::Reverse, collections::BinaryHeap, sync::Arc, time::Duration, u32, usize};

use rand::Rng;
use tokio::{sync::Mutex, time::Instant};

const EVICTION_MAX_NUMBER: usize = 5;

use crate::{
    core_time::get_cached_time_ms,
    db::{
        Storage,
        eviction::{LockOwner, MemoryCache, NUM_SHARDS},
    },
};

impl Storage {
    /**
     * 淘汰逻辑
     */
    pub async fn eviction_ttl(self, shutdown_tx: tokio::sync::broadcast::Sender<()>) {
        let mut receiver = shutdown_tx.subscribe();
        loop {
            //如果通知来了 就会下次循环会直接break
            tokio::select! {
                _ = receiver.recv() =>{
                    break;
                },
                _ = tokio::time::sleep(Duration::from_millis(100)) =>{

                }
            }
            //先创建一个数组存储
            let mut active_shards: Vec<(usize, usize)> = Vec::new();
            for db_index in 0..16 {
                for shard_index in 0..NUM_SHARDS {
                    let shard = self.get_lock_read(db_index, shard_index).await;
                    if shard.get_memory_usage() > 0 {
                        active_shards.push((db_index, shard_index));
                    }
                }
            }
            if active_shards.is_empty() {
                continue;
            }

            let mut keys_check = 20;

            while keys_check > 0 && !active_shards.is_empty() {
                // 5. 从“活跃分片列表”中随机挑一个
                let random_active_index = rand::thread_rng().gen_range(0..active_shards.len());
                let (db_index, shard_index) = active_shards[random_active_index];
                //抽取后获取锁
                let mut shard = self.get_lock_write(db_index, shard_index).await;
                //获取锁后再次判断 如果没有数据就跳过了
                if shard.get_memory_usage() == 0 {
                    //说明这个分片已经没有数据了
                    active_shards.swap_remove(random_active_index);
                    continue;
                }
                // 提取到单独的变量，让 get_eviction_policy 返回的 MutexGuard 及时释放，避免借用冲突
                let key_opt = shard.get_eviction_policy().unwrap().get_random_sample_key();
                if let Some(key) = key_opt {
                    if let Some(value) = shard.select(&key).await {
                        if let Some(expire_time) = value.expires_at {
                            if get_cached_time_ms() > expire_time {
                                // 调用统一的 delete 方法，内部会处理内存和 LRU 移除
                                shard.delete(&key).await;
                            }
                        }
                    }
                }
                keys_check -= 1;
            }
        }
    }

    //这个方法有点长 由于这个是内存管理专门的方法 不用复用了 直接写在一起了
    pub async fn eviction_memory(
        self,
        target_memory: usize,
        shutdown_tx: tokio::sync::broadcast::Sender<()>,
    ) -> Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> {
        let shutdown_clone = shutdown_tx.clone();
        let task_vec: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> = Arc::new(Vec::new().into());
        let task_vec_clone = task_vec.clone();
        //定时任务接收者
        loop {
            let mut shutdown = shutdown_clone.clone().subscribe();
            let is_over_memory = crate::db::eviction::GLOBAL_MEMORY
                .load(std::sync::atomic::Ordering::Relaxed)
                > target_memory;

            if !is_over_memory {
                // 如果内存健康，休眠 100ms 慢慢巡逻
                tokio::select! {
                    _ = shutdown.recv() =>{
                        break;
                    },
                    _ = tokio::time::sleep(Duration::from_millis(100)) =>{

                    }
                }
            } else {
                // 如果内存溢出，不要休眠！只需无阻塞检查一下是否关机，然后立刻去清理
                if let Ok(_) = shutdown.try_recv() {
                    break;
                }
            }

            if is_over_memory {
                //根据指标挑选前五的分片
                let mut shard_indices: BinaryHeap<Reverse<(usize, usize, usize)>> =
                    BinaryHeap::with_capacity(EVICTION_MAX_NUMBER);
                for db_index in 0..16 {
                    for shard_index in 0..NUM_SHARDS {
                        let shard = self.get_lock_read(db_index, shard_index).await;
                        let memory = shard.get_memory_usage();
                        //跳过为空的
                        if memory == 0 {
                            continue;
                        }
                        // 2. 创建元组“值”
                        let tuple_value = (memory, db_index, shard_index);
                        // 3. 使用圆括号 () 把“值”包装起来
                        //    这创建了一个 Reverse<(usize, usize, usize)> 类型的 *值*
                        let item_for_heap = Reverse(tuple_value);
                        shard_indices.push(item_for_heap);
                    }
                }
                let mut handles = Vec::new();
                for item in shard_indices {
                    //每次循环都需要克隆
                    let shutdown_clone = shutdown_tx.clone();
                    let (_, db_index, shard_index) = item.0;
                    let store: Arc<Vec<Arc<MemoryCache>>> = self.store.clone();
                    // 内存超了，开一个任务
                    let task_delete = tokio::spawn(async move {
                        let mut processed_count = 0;
                        let start_stopwatch = Instant::now();
                        let time_budget = Duration::from_millis(20);
                        loop {
                            let mut shutdown = shutdown_clone.clone().subscribe();
                            if let Ok(_) = shutdown.try_recv() {
                                break;
                            }
                            if crate::db::eviction::GLOBAL_MEMORY
                                .load(std::sync::atomic::Ordering::Relaxed)
                                <= target_memory
                            {
                                break;
                            }

                            // 在循环内部获取【写锁】，这样不会长期阻塞整个分片
                            let shard_lock_guard = store[db_index].message[shard_index]
                                .clone()
                                .write_owned()
                                .await;
                            let mut shard_operator: Box<dyn crate::db::eviction::LockOwner> =
                                Box::new(crate::db::eviction::DirectCacheNode::Writeguard(
                                    shard_lock_guard,
                                ));

                            let mut deleted_in_batch = 0;
                            // 将批量删除提高到 200，减少 yield 带来的 tokio 上下文切换开销
                            while deleted_in_batch < 200
                                && crate::db::eviction::GLOBAL_MEMORY
                                    .load(std::sync::atomic::Ordering::Relaxed)
                                    > target_memory
                            {
                                let key =
                                    shard_operator.get_eviction_policy().unwrap().pop_victim();
                                if let Some(key) = key {
                                    // 统一的 delete 方法会搞定 LRU 链表和内存记账
                                    shard_operator.delete(&key).await;
                                    deleted_in_batch += 1;
                                    processed_count += 1;
                                } else {
                                    break;
                                }
                            }

                            // 主动让出 CPU 给其他任务
                            tokio::task::yield_now().await;

                            if deleted_in_batch == 0 {
                                break; // 这个分片已经没东西可删了
                            }

                            // 精准判断时间
                            if start_stopwatch.elapsed() > time_budget {
                                break;
                            }
                        }
                    });
                    handles.push(task_delete);
                }

                // 【关键修复】等待这批清理任务执行完毕！
                // 否则如果内存没降下来，下一次 100ms 循环又会 spawn 5 个新任务
                // 导致任务无限爆炸，最终耗尽 CPU 资源，让 SET 性能衰减到冰点
                for handle in handles {
                    let _ = handle.await;
                }
            }
        }
        task_vec
    }
    //移除了容易导致死锁的 get_global_memory_can_move 和 get_global_memory_not_move
}
