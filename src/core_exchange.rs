use crate::domain::MGetCommand;
use crate::domain::MSetCommand;
use crate::error::KvError::ProtocolError;
use crate::error::{
    BLPopCommand, Command, EvalCommand, Frame, GetCommand, HDelCommand, HGetCommand, HSetCommand,
    KvError, LPopCommand, LPushCommand, PingCommand, SetCommand, UnimplementCommand,
};
use crate::parser::Parser;

impl TryFrom<Frame> for Command {
    type Error = KvError;

    //就是类似构造函数的东西
    fn try_from(frame: Frame) -> Result<Self, Self::Error> {
        let frames = match frame {
            Frame::Array(frames) => frames,
            _ => return Err(ProtocolError("must be a array from frame".into())),
        };
        if frames.is_empty() {
            return Err(ProtocolError("frame is empty".into()));
        }
        let length = frames.len();
        let mut iter: std::vec::IntoIter<Frame> = frames.into_iter();
        match iter.next() {
            Some(Frame::Bulk(start_str)) => {
                let command_name = String::from_utf8(start_str.to_vec())
                    .map_err(|_| ProtocolError("zhuan huan yi chang ".into()))?
                    .to_uppercase();
                match command_name.as_str() {
                    "GET" => {
                        if length != 2 {
                            return Err(ProtocolError("GET 命令需要 1 个参数".into()));
                        }
                        GetCommand::parse(iter, command_name)
                    }
                    "SET" => {
                        if length < 3 {
                            return Err(ProtocolError("frame is too short".into()));
                        }
                        SetCommand::parse(iter, command_name)
                    }
                    "MSET" => {
                        if length < 3 || length % 2 == 0 {
                            return Err(ProtocolError("MSET 命令参数错误".into()));
                        }
                        MSetCommand::parse(iter, command_name)
                    }
                    "MGET" => {
                        if length < 2 {
                            return Err(ProtocolError("MGET 命令至少需要 1 个参数".into()));
                        }
                        MGetCommand::parse(iter, command_name)
                    }
                    "LPUSH" => {
                        if length < 3 {
                            return Err(ProtocolError("LPUSH 命令需要至少 2 个参数".into()));
                        }
                        LPushCommand::parse(iter, command_name)
                    }
                    "LPOP" => {
                        if length != 2 {
                            return Err(ProtocolError("LPOP 命令需要 1 个参数".into()));
                        }
                        LPopCommand::parse(iter, command_name)
                    }
                    "BLPOP" => {
                        if length != 3 {
                            return Err(ProtocolError("BLPOP 命令需要 2 个参数".into()));
                        }
                        BLPopCommand::parse(iter, command_name)
                    }
                    "HSET" => {
                        if length < 4 || length % 2 != 0 {
                            return Err(ProtocolError("HSET 命令参数错误".into()));
                        }
                        HSetCommand::parse(iter, command_name)
                    }
                    "HGET" => {
                        if length != 3 {
                            return Err(ProtocolError("HGET 命令需要 2 个参数".into()));
                        }
                        HGetCommand::parse(iter, command_name)
                    }
                    "HDEL" => {
                        if length < 3 {
                            return Err(ProtocolError("HDEL 命令需要至少 2 个参数".into()));
                        }
                        HDelCommand::parse(iter, command_name)
                    }
                    "JSON.SET" => {
                        if length != 4 {
                            return Err(ProtocolError("JSON.SET 命令需要 3 个参数".into()));
                        }
                        crate::domain::command::JsonSetCommand::parse(iter, command_name)
                    }
                    "JSON.GET" => {
                        if length != 3 {
                            return Err(ProtocolError("JSON.GET 命令需要 2 个参数".into()));
                        }
                        crate::domain::command::JsonGetCommand::parse(iter, command_name)
                    }
                    "ZADD" => {
                        if length != 4 {
                            return Err(ProtocolError("ZADD 命令需要 3 个参数".into()));
                        }
                        crate::domain::command::ZAddCommand::parse(iter, command_name)
                    }
                    "ZSCORE" => {
                        if length != 3 {
                            return Err(ProtocolError("ZSCORE 命令需要 2 个参数".into()));
                        }
                        crate::domain::command::ZScoreCommand::parse(iter, command_name)
                    }
                    "ZRANK" => {
                        if length != 3 {
                            return Err(ProtocolError("ZRANK 命令需要 2 个参数".into()));
                        }
                        crate::domain::command::ZRankCommand::parse(iter, command_name)
                    }
                    "ZRANGE" => {
                        if length != 4 {
                            return Err(ProtocolError("ZRANGE 命令需要 3 个参数".into()));
                        }
                        crate::domain::command::ZRangeCommand::parse(iter, command_name)
                    }
                    "ZREM" => {
                        if length != 3 {
                            return Err(ProtocolError("ZREM 命令需要 2 个参数".into()));
                        }
                        crate::domain::command::ZRemCommand::parse(iter, command_name)
                    }
                    "PING" => PingCommand::parse(iter, command_name),
                    "MULTI" => Ok(Command::Multi(crate::domain::command::MultiCommand {})),
                    "EXEC" => Ok(Command::Exec(crate::domain::command::ExecCommand {})),
                    //lua 脚本
                    "EVAL" => EvalCommand::parse(iter, command_name),

                    // 4. 所有其他不认识的命令，都匹配到这里
                    _ => UnimplementCommand::parse(iter, command_name),
                }
            }
            _ => Err(ProtocolError("not a command".into())),
        }
    }
}
