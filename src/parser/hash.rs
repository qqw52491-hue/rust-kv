use crate::domain::{Command, Frame, HDelCommand, HGetCommand, HSetCommand, KvError};
use crate::parser::{Parser, extract_bulk_bytes, extract_bulk_string};
use bytes::Bytes;
use std::sync::Arc;
use std::vec::IntoIter;

impl Parser for HSetCommand {
    fn parse(mut itor: IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
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

impl Parser for HGetCommand {
    fn parse(mut itor: IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
        let key = Arc::new(extract_bulk_string(itor.next())?);
        let field = extract_bulk_string(itor.next())?;
        Ok(Command::HGet(HGetCommand { key, field }))
    }
}

impl Parser for HDelCommand {
    fn parse(mut itor: IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
        let key = Arc::new(extract_bulk_string(itor.next())?);
        let mut fields = Vec::new();
        for frame in itor {
            fields.push(extract_bulk_string(Some(frame))?);
        }
        Ok(Command::HDel(HDelCommand { key, fields }))
    }
}
