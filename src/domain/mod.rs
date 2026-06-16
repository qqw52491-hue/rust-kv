pub mod command;
pub mod error;
pub mod protocol;
pub mod types;

// Re-export core types for ergonomic usage across the codebase
pub use command::{
    Command, EvalCommand, Expiration, GetCommand, LPopCommand, LPushCommand, LockSpec, PingCommand,
    SetCommand, SetCondition, UnimplementCommand, HSetCommand, HGetCommand, HDelCommand, MSetCommand,
};
pub use error::KvError;
pub use protocol::{Frame, IsAof, ToBulk};
pub use types::{Element, Value, ValueEntry};
