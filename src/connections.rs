use std::io;
use crate::commands;
use crate::core;
use crate::core::resp;

pub fn apply(
    _state:  &core::StateContext,
    command: &commands::ConnectionManagement
) -> io::Result<resp::Message> {
    match command {
        commands::ConnectionManagement::SetClientName(_name) => 
            Ok(resp::Message::SimpleString("OK".to_string())),
        commands::ConnectionManagement::SelectDatabase(_database) => 
            Ok(resp::Message::SimpleString("OK".to_string())),
        commands::ConnectionManagement::Ping(message) => 
            Ok(resp::Message::SimpleString(message.clone())),
    }
}