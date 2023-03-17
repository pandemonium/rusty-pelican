use std::io;
use crate::commands;
use crate::core;
use crate::resp;
use crate::resp::Message;

pub fn apply(
    state:   &core::State, 
    command: commands::ConnectionManagement
) -> Result<resp::Message, io::Error> {
    match command {
        commands::ConnectionManagement::SetClientName(_name) => 
            Ok(Message::SimpleString("OK".to_string())),
        commands::ConnectionManagement::SelectDatabase(_database) => 
            Ok(Message::SimpleString("OK".to_string())),
        commands::ConnectionManagement::Ping(message) => 
            Ok(Message::SimpleString(message.to_string())),
    }
}