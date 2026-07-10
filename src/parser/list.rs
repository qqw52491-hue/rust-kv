use std::{sync::Arc, vec::IntoIter};

use crate::{
    parser::{extract_bulk_bytes, extract_bulk_string, Parser},
    error::{Command, Frame, KvError, LPushCommand, LPopCommand},
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
        Ok(Command::LPop(LPopCommand {
            key: Arc::new(key),
        }))
    }
}
