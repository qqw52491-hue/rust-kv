use crate::domain::command::{JsonGetCommand, JsonSetCommand};
use crate::error::{Command, Frame, KvError};
use crate::parser::{Parser, extract_bulk_string};
use std::sync::Arc;

impl Parser for JsonSetCommand {
    fn parse(
        mut iter: std::vec::IntoIter<Frame>,
        _command_name: String,
    ) -> Result<Command, KvError> {
        let key = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("JSON.SET missing key".into())),
        };
        let path = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("JSON.SET missing path".into())),
        };
        let value = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("JSON.SET missing value".into())),
        };

        Ok(Command::JsonSet(JsonSetCommand {
            key: Arc::new(key),
            path,
            value,
        }))
    }
}

impl Parser for JsonGetCommand {
    fn parse(
        mut iter: std::vec::IntoIter<Frame>,
        _command_name: String,
    ) -> Result<Command, KvError> {
        let key = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("JSON.GET missing key".into())),
        };
        let path = match iter.next() {
            Some(frame) => extract_bulk_string(Some(frame))?,
            None => return Err(KvError::ProtocolError("JSON.GET missing path".into())),
        };

        Ok(Command::JsonGet(JsonGetCommand {
            key: Arc::new(key),
            path,
        }))
    }
}
