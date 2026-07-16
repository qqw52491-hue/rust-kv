use bytes::Bytes;
use std::sync::Arc;

// ─────────────────────────────────────────────
// 顶层命令枚举：所有客户端可以发送的命令变体
// ─────────────────────────────────────────────
#[derive(Debug, Clone)]
pub enum Command {
    Set(SetCommand),
    Get(GetCommand),
    Ping(PingCommand),
    Unimplement(UnimplementCommand),
    EvalCommand(EvalCommand),
    LPush(LPushCommand),
    LPop(LPopCommand),
    BLPop(BLPopCommand),
    HSet(HSetCommand),
    HGet(HGetCommand),
    HDel(HDelCommand),
    MSet(MSetCommand),
    MGet(MGetCommand),
    Multi(MultiCommand),
    Exec(ExecCommand),
    MultiGroup(Vec<Command>),
    JsonSet(JsonSetCommand),
    JsonGet(JsonGetCommand),
    ZAdd(ZAddCommand),
    ZScore(ZScoreCommand),
    ZRank(ZRankCommand),
    ZRange(ZRangeCommand),
    ZRem(ZRemCommand),
}

// ─────────────────────────────────────────────
// 各命令结构体：每个都是独立、清晰的命令"实体"
// ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SetCommand {
    pub key: Arc<String>,
    pub value: Bytes,
    pub expiration: Option<Expiration>,
    pub condition: Option<SetCondition>,
}

#[derive(Debug, Clone)]
pub struct MSetCommand {
    pub keys_and_values: Vec<(Arc<String>, Bytes)>,
}

#[derive(Debug, Clone)]
pub struct MGetCommand {
    pub keys: Vec<Arc<String>>,
}

#[derive(Debug, Clone)]
pub struct GetCommand {
    pub key: Arc<String>,
}

#[derive(Debug, Clone)]
pub struct HSetCommand {
    pub key: Arc<String>,
    pub field_values: Vec<(String, Bytes)>,
}

#[derive(Debug, Clone)]
pub struct HGetCommand {
    pub key: Arc<String>,
    pub field: String,
}

#[derive(Debug, Clone)]
pub struct HDelCommand {
    pub key: Arc<String>,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PingCommand {
    pub value: Option<String>,
}

#[derive(Debug, Clone)]
pub struct UnimplementCommand {
    pub command: String,
    pub args: Vec<Bytes>,
}

#[derive(Debug, Clone)]
pub struct EvalCommand {
    pub script: String,
    pub keys: Vec<String>,
    pub args: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct LPushCommand {
    pub key: Arc<String>,
    pub values: Vec<Bytes>,
}

#[derive(Debug, Clone)]
pub struct LPopCommand {
    pub key: Arc<String>,
}

#[derive(Debug, Clone)]
pub struct BLPopCommand {
    pub key: Arc<String>,
    pub timeout: u64, // seconds
}

#[derive(Debug, Clone)]
pub struct MultiCommand {}

#[derive(Debug, Clone)]
pub struct ExecCommand {}

#[derive(Debug, Clone)]
pub struct JsonSetCommand {
    pub key: Arc<String>,
    pub path: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct JsonGetCommand {
    pub key: Arc<String>,
    pub path: String,
}

#[derive(Debug, Clone)]
pub struct ZAddCommand {
    pub key: Arc<String>,
    pub score: f64,
    pub member: bytes::Bytes,
}

#[derive(Debug, Clone)]
pub struct ZScoreCommand {
    pub key: Arc<String>,
    pub member: bytes::Bytes,
}

#[derive(Debug, Clone)]
pub struct ZRankCommand {
    pub key: Arc<String>,
    pub member: bytes::Bytes,
}

#[derive(Debug, Clone)]
pub struct ZRangeCommand {
    pub key: Arc<String>,
    pub start: isize,
    pub stop: isize,
}

#[derive(Debug, Clone)]
pub struct ZRemCommand {
    pub key: Arc<String>,
    pub member: bytes::Bytes,
}

// ─────────────────────────────────────────────
// 辅助类型：SET 命令专属
// ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Expiration {
    EX(u64),   // 秒
    PX(u64),   // 毫秒
    EXAT(u64), // Unix 时间戳（秒）
    PXAT(u64), // Unix 时间戳（毫秒）
}

#[derive(Debug, Clone)]
pub enum SetCondition {
    NX, // Not Exists
    XX, // Exists
}

// ─────────────────────────────────────────────
// 锁规格：声明命令需要哪种分片锁（以及锁哪个 key）
// ─────────────────────────────────────────────

#[derive(Debug)]
pub enum LockSpec<'a> {
    Write(&'a Arc<String>),
    Read(&'a Arc<String>),
    None,
}

impl Command {
    /// 返回该命令所需的锁类型与目标 key。
    /// 这是分片锁分配的唯一权威来源。
    pub fn lock_spec(&self) -> LockSpec<'_> {
        match self {
            Command::Set(c) => LockSpec::Write(&c.key),
            Command::Get(c) => LockSpec::Read(&c.key),
            Command::LPush(c) => LockSpec::Write(&c.key),
            Command::LPop(c) => LockSpec::Write(&c.key),
            Command::BLPop(c) => LockSpec::Write(&c.key),
            Command::HSet(c) => LockSpec::Write(&c.key),
            Command::HGet(c) => LockSpec::Read(&c.key),
            Command::HDel(c) => LockSpec::Write(&c.key),
            Command::MSet(_) => LockSpec::None,
            Command::MGet(_) => LockSpec::None,
            Command::Ping(_) => LockSpec::None,
            Command::Unimplement(_) => LockSpec::None,
            Command::EvalCommand(_) => LockSpec::None,
            Command::Multi(_) => LockSpec::None,
            Command::Exec(_) => LockSpec::None,
            Command::MultiGroup(_) => LockSpec::None,
            Command::JsonSet(c) => LockSpec::Write(&c.key),
            Command::JsonGet(c) => LockSpec::Read(&c.key),
            Command::ZAdd(c) => LockSpec::Write(&c.key),
            Command::ZScore(c) => LockSpec::Read(&c.key),
            Command::ZRank(c) => LockSpec::Read(&c.key),
            Command::ZRange(c) => LockSpec::Read(&c.key),
            Command::ZRem(c) => LockSpec::Write(&c.key),
        }
    }

    /// 返回命令关联的 key（从 lock_spec 推导，不重复写 match）。
    pub fn get_key(&self) -> Option<&Arc<String>> {
        match self.lock_spec() {
            LockSpec::Write(key) | LockSpec::Read(key) => Some(key),
            LockSpec::None => None,
        }
    }
}
