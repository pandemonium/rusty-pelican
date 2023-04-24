use std::io;
use crate::commands;
use crate::core;
use crate::core::resp;
use crate::generic::*;
use crate::core::snapshots::Snapshots;

pub fn apply(
    state:   &core::StateContext,
    command: &commands::ServerManagement
) -> io::Result<resp::Message> {
    match command {
        commands::ServerManagement::DbSize =>
            Ok(resp::Message::Integer(
                state.begin_reading()?.filter_keys("*").len() as i64
            )),
        commands::ServerManagement::Command(_options) =>
            Ok(resp::Message::Error {
                prefix: resp::ErrorPrefix::Err,
                message: "Unsupported command".to_string(),
            }),
        commands::ServerManagement::Info(commands::Topic::Keyspace) => {
            let keys = state.begin_reading()?.filter_keys("*");
            let keyspace = format!("# Keyspace\r\ndb0:keys={},expires=0,avg_ttl=0\r\n", keys.len());
            Ok(resp::Message::BulkString(keyspace))
        },
        commands::ServerManagement::Info(commands::Topic::Server) =>
            Ok(resp::Message::BulkString(
                "# Server\r\nredis_version:7.0.9\r\n".to_string()
            )),
        commands::ServerManagement::Info(commands::Topic::Named(topic)) =>
            Ok(resp::Message::BulkString(format!("Info about {topic}"))),
//            Ok(resp::Message::Error {
//                prefix: resp::ErrorPrefix::Err,
//                message: "Unsupported command".to_string(),
//            }),
        commands::ServerManagement::BgSave => {
            /* thread::spawn(move || ... ) */
            state.begin_reading()?.save_snapshot()?;
            Ok(resp::Message::SimpleString("OK".to_string()))
        },
}
}