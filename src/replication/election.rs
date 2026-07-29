use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Master,
    Slave,
    Candidate,
}

/// 全局选举状态
pub struct ElectionState {
    /// 当前节点的角色
    pub role: RwLock<Role>,
    /// 当前选举任期 (Term)
    pub current_term: AtomicU64,
    /// 记录当前公认的老大是谁 (IP:PORT)
    pub leader_id: RwLock<Option<String>>,
    /// 记录在当前任期内，我把票投给了谁 (防止一仆二主)
    pub voted_for: RwLock<Option<String>>,
    /// 记录最后一次收到老大心跳的时间戳 (毫秒)
    pub last_heartbeat: AtomicU64,
    /// 自己的节点 ID (或者地址)
    pub my_id: String,
}

impl ElectionState {
    pub fn new(initial_role: Role, my_id: String) -> Self {
        Self {
            role: RwLock::new(initial_role),
            current_term: AtomicU64::new(0),
            leader_id: RwLock::new(None),
            voted_for: RwLock::new(None),
            last_heartbeat: AtomicU64::new(Self::now_ms()),
            my_id,
        }
    }

    /// 更新心跳时间
    pub fn update_heartbeat(&self) {
        self.last_heartbeat.store(Self::now_ms(), Ordering::Release);
    }

    /// 检查是否选举超时 (例如超过 2000 毫秒没有心跳)
    pub fn is_timeout(&self, timeout_ms: u64) -> bool {
        let now = Self::now_ms();
        let last = self.last_heartbeat.load(Ordering::Acquire);
        now.saturating_sub(last) > timeout_ms
    }

    /// 获取当前毫秒时间戳
    pub fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

use once_cell::sync::Lazy;

/// 全局静态实例
pub static GLOBAL_ELECTION: Lazy<Arc<ElectionState>> = Lazy::new(|| {
    // 初始状态根据配置文件先简单决定：如果配了 replica_of 就是 Slave，否则是 Master
    let initial_role = if crate::config::CONFIG.replica_of.is_none() {
        Role::Master
    } else {
        Role::Slave
    };
    // 临时用 server_addr 来当做 my_id (如果是正式集群，需要配置唯一的节点 ID)
    let my_id = crate::config::CONFIG.server_addr.clone();

    Arc::new(ElectionState::new(initial_role, my_id))
});

use rand::Rng;
use tokio::time::{Duration, sleep};

/// 启动后台选举监控循环
pub fn start_election_loop() {
    tokio::spawn(async move {
        loop {
            // 每次循环稍微等一会儿，减轻 CPU 压力
            sleep(Duration::from_millis(100)).await;

            let role = GLOBAL_ELECTION.role.read().await.clone();

            // 只有处于 Slave 或者 Candidate 状态时才需要倒计时
            if role == Role::Slave || role == Role::Candidate {
                // 引入随机超时时间 (比如 1500 毫秒到 3000 毫秒)，防止所有人同时发起选举
                let random_timeout = {
                    let mut rng = rand::thread_rng();
                    rng.gen_range(1500..3000)
                };

                if GLOBAL_ELECTION.is_timeout(random_timeout) {
                    tracing::warn!(
                        "警告：超过 {} 毫秒没收到老大的心跳！准备发起造反...",
                        random_timeout
                    );

                    // 1. 改变身份为候选人
                    *GLOBAL_ELECTION.role.write().await = Role::Candidate;

                    // 2. 任期号 (Term) + 1
                    let new_term = GLOBAL_ELECTION.current_term.fetch_add(1, Ordering::SeqCst) + 1;

                    // 2.5 清空上一届的投票记录，并立刻把这一届的第一票投给自己！
                    *GLOBAL_ELECTION.voted_for.write().await = Some(GLOBAL_ELECTION.my_id.clone());

                    tracing::info!(
                        "我是候选人，当前选举任期变为: {}，开始给自己投票！",
                        new_term
                    );

                    // 3. 更新自己的心跳时间，重新开始倒计时（防止这次没选上，还能重新选）
                    GLOBAL_ELECTION.update_heartbeat();

                    // TODO: 给其他所有节点发送 RAFT.VOTE 消息拉票！
                }
            }
        }
    });
}
