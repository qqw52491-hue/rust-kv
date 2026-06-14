use std::vec::IntoIter;
use bytes::Bytes;
use std::sync::Arc;
use crate::domain::{Command, Frame, KvError, HSetCommand, HGetCommand, HDelCommand};
use crate::command_exchange::{CommandExchange, extract_bulk_string, extract_bulk_bytes};

impl CommandExchange for HSetCommand {
    fn exchange(mut itor: IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
        let key = Arc::new(extract_bulk_string(itor.next())?);
        let mut field_values = Vec::new();
        while let Some(frame_k) = itor.next() {
            let field = extract_bulk_string(Some(frame_k))?;
            let value = extract_bulk_bytes(itor.next())?;
            field_values.push((field, value));
        }
        Ok(Command::HSet(HSetCommand { key, field_values }))
    }
}

impl CommandExchange for HGetCommand {
    fn exchange(mut itor: IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
        let key = Arc::new(extract_bulk_string(itor.next())?);
        let field = extract_bulk_string(itor.next())?;
        Ok(Command::HGet(HGetCommand { key, field }))
    }
}

impl CommandExchange for HDelCommand {
    fn exchange(mut itor: IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
        let key = Arc::new(extract_bulk_string(itor.next())?);
        let mut fields = Vec::new();
        for frame in itor {
            fields.push(extract_bulk_string(Some(frame))?);
        }
        Ok(Command::HDel(HDelCommand { key, fields }))
    }
}
