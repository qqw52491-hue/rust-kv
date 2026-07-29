use std::{sync::Arc, vec::IntoIter};

use crate::{
    error::{BLPopCommand, Command, Frame, KvError, LPopCommand, LPushCommand},
    parser::{Parser, extract_bulk_bytes, extract_bulk_string},
};

impl Parser for LPushCommand {
    fn parse(mut itor: IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
        let key = extract_bulk_string(itor.next())?;
        let mut values = Vec::new();
        for frame in itor {
            values.push(extract_bulk_bytes(Some(frame))?);
        }
        Ok(Command::LPush(LPushCommand {
            key: Arc::new(key),
            values,
        }))
    }
}

impl Parser for LPopCommand {
    fn parse(mut itor: IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
        let key = extract_bulk_string(itor.next())?;
        Ok(Command::LPop(LPopCommand { key: Arc::new(key) }))
    }
}

impl Parser for BLPopCommand {
    fn parse(mut itor: IntoIter<Frame>, _command_name: String) -> Result<Command, KvError> {
        let key = extract_bulk_string(itor.next())?;
        let timeout_str = extract_bulk_string(itor.next())?;
        let timeout = timeout_str
            .parse::<u64>()
            .map_err(|_| KvError::ProtocolError("Invalid timeout".into()))?;
        Ok(Command::BLPop(BLPopCommand {
            key: Arc::new(key),
            timeout,
        }))
    }
}
