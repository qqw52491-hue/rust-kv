use once_cell::sync::Lazy;
use std::env;
use dotenvy::dotenv;

pub struct Config {
    pub eviction_type: EvictionType,
    pub server_addr: String,
    pub aof_file_path: String,
    pub lua_worker_count: usize,
    pub lua_queue_depth: usize,
    pub lua_vm_pool_size: usize,
    pub num_shards: usize,
    pub replica_of: Option<String>,
    pub cluster_peers: Vec<String>,
    pub aof_fsync: AofFsync,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum AofFsync {
    Always,
    EverySec,
    No,
}

pub enum EvictionType {
    LRU,
    LFU,
}

pub static CONFIG: Lazy<Config> = Lazy::new(|| {
    // 尝试加载 .env 文件，如果文件不存在也不会 panic
    let _ = dotenv();

    println!("--- Loading configuration ---");

    // 从环境变量中读取，如果没有配置则默认回退到 "LRU"
    let policy_str = env::var("EVICTION_POLICY").unwrap_or_else(|_| "LRU".to_string());
    
    let eviction_type = match policy_str.to_uppercase().as_str() {
        "LFU" => {
            println!("Eviction Policy set to: LFU");
            EvictionType::LFU
        },
        "LRU" | _ => {
            println!("Eviction Policy set to: LRU");
            EvictionType::LRU
        },
    };

    let fsync_str = env::var("AOF_FSYNC").unwrap_or_else(|_| "EverySec".to_string());
    let aof_fsync = match fsync_str.to_uppercase().as_str() {
        "ALWAYS" => {
            println!("AOF Fsync Policy set to: Always (Max Durability, Low QPS)");
            AofFsync::Always
        },
        "NO" => {
            println!("AOF Fsync Policy set to: No (OS controlled, Max QPS)");
            AofFsync::No
        },
        "EVERYSEC" | _ => {
            println!("AOF Fsync Policy set to: EverySec (Balanced, Default)");
            AofFsync::EverySec
        },
    };

    let server_addr = env::var("SERVER_ADDR").unwrap_or_else(|_| "0.0.0.0:6380".to_string());
    let aof_file_path = env::var("AOF_FILE_PATH").unwrap_or_else(|_| "database.aof".to_string());
    let replica_of = env::var("REPLICA_OF").ok().filter(|s| !s.trim().is_empty());
    if let Some(ref master) = replica_of {
        println!("Role set to: SLAVE (Replicating from Master: {})", master);
    } else {
        println!("Role set to: MASTER");
    }

    let cluster_peers = env::var("CLUSTER_PEERS")
        .map(|s| s.split(',').map(|p| p.trim().to_string()).filter(|p| !p.is_empty()).collect())
        .unwrap_or_else(|_| Vec::new());
    if !cluster_peers.is_empty() {
        println!("Cluster Peers Loaded: {:?}", cluster_peers);
    }

    let lua_worker_count = env::var("LUA_WORKER_COUNT")
        .unwrap_or_else(|_| "8".to_string())
        .parse()
        .unwrap_or(8);
        
    let lua_queue_depth = env::var("LUA_QUEUE_DEPTH")
        .unwrap_or_else(|_| "100000".to_string())
        .parse()
        .unwrap_or(100000);
        
    let lua_vm_pool_size = env::var("LUA_VM_POOL_SIZE")
        .unwrap_or_else(|_| "50".to_string())
        .parse()
        .unwrap_or(50);
        
    let num_shards = env::var("NUM_SHARDS")
        .unwrap_or_else(|_| "64".to_string())
        .parse()
        .unwrap_or(64);

    Config {
        eviction_type,
        server_addr,
        aof_file_path,
        lua_worker_count,
        lua_queue_depth,
        lua_vm_pool_size,
        num_shards,
        replica_of,
        cluster_peers,
        aof_fsync,
    }
});
