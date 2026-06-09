use crate::command_exchange::CommandExchange;
use crate::error::KvError::ProtocolError;
use crate::error::{
    Command, EvalCommand, Frame, GetCommand, KvError, PingCommand, SetCommand, UnimplementCommand,
    LPushCommand, LPopCommand,
};

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
                        GetCommand::exchange(iter, command_name)
                    }
                    "SET" => {
                        if length < 3 {
                            return Err(ProtocolError("frame is too short".into()));
                        }
                        SetCommand::exchange(iter, command_name)
                    }
                    "LPUSH" => {
                        if length < 3 {
                            return Err(ProtocolError("LPUSH 命令需要至少 2 个参数".into()));
                        }
                        LPushCommand::exchange(iter, command_name)
                    }
                    "LPOP" => {
                        if length != 2 {
                            return Err(ProtocolError("LPOP 命令需要 1 个参数".into()));
                        }
                        LPopCommand::exchange(iter, command_name)
                    }
                    "PING" => PingCommand::exchange(iter, command_name),
                    //lua 脚本
                    "EVAL" => EvalCommand::exchange(iter, command_name),

                    // 4. 所有其他不认识的命令，都匹配到这里
                    _ => UnimplementCommand::exchange(iter, command_name),
                }
            }
            _ => Err(ProtocolError("not a command".into())),
        }
    }
}
