pub mod command;
pub mod error;
pub mod protocol;
pub mod types;

// Re-export core types for ergonomic usage across the codebase
pub use command::{
    BLPopCommand, Command, EvalCommand, ExecCommand, Expiration, GetCommand, HDelCommand,
    HGetCommand, HSetCommand, LPopCommand, LPushCommand, LockSpec, MGetCommand, MSetCommand,
    MultiCommand, PingCommand, SetCommand, SetCondition, UnimplementCommand,
};
pub use error::KvError;
pub use protocol::{Frame, IsAof, ToBulk};
pub use types::{Element, Value, ValueEntry};
