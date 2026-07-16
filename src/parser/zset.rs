use std::sync::Arc;
use crate::error::{Command, Frame, KvError};
use crate::domain::command::{ZAddCommand, ZScoreCommand, ZRankCommand, ZRangeCommand, ZRemCommand};
use crate::parser::Parser;
use crate::parser::extract_bulk_string;

impl Parser for ZAddCommand {
    fn parse(mut iter: std::vec::IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
        let key = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("ZADD missing key".into())),
        };
        let score_str = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("ZADD missing score".into())),
        };
        let score = score_str.parse::<f64>().map_err(|_| KvError::ProtocolError("ZADD score is not a valid float".into()))?;
        let member = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("ZADD missing member".into())),
        };

        Ok(Command::ZAdd(ZAddCommand {
            key: Arc::new(key),
            score,
            member: bytes::Bytes::from(member),
        }))
    }
}

impl Parser for ZScoreCommand {
    fn parse(mut iter: std::vec::IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
        let key = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("ZSCORE missing key".into())),
        };
        let member = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("ZSCORE missing member".into())),
        };
        Ok(Command::ZScore(ZScoreCommand { key: Arc::new(key), member: bytes::Bytes::from(member) }))
    }
}

impl Parser for ZRankCommand {
    fn parse(mut iter: std::vec::IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
        let key = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("ZRANK missing key".into())),
        };
        let member = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("ZRANK missing member".into())),
        };
        Ok(Command::ZRank(ZRankCommand { key: Arc::new(key), member: bytes::Bytes::from(member) }))
    }
}

impl Parser for ZRangeCommand {
    fn parse(mut iter: std::vec::IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
        let key = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("ZRANGE missing key".into())),
        };
        let start_str = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("ZRANGE missing start".into())),
        };
        let start = start_str.parse::<isize>().map_err(|_| KvError::ProtocolError("ZRANGE start must be integer".into()))?;
        
        let stop_str = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("ZRANGE missing stop".into())),
        };
        let stop = stop_str.parse::<isize>().map_err(|_| KvError::ProtocolError("ZRANGE stop must be integer".into()))?;
        
        Ok(Command::ZRange(ZRangeCommand { key: Arc::new(key), start, stop }))
    }
}

impl Parser for ZRemCommand {
    fn parse(mut iter: std::vec::IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
        let key = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("ZREM missing key".into())),
        };
        let member = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("ZREM missing member".into())),
        };
        Ok(Command::ZRem(ZRemCommand { key: Arc::new(key), member: bytes::Bytes::from(member) }))
    }
}
