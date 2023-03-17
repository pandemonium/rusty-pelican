use std::convert::TryFrom;
use std::io;
use std::fmt;
use std::str;
use crate::resp::*;


#[derive(Debug, PartialEq)]
pub enum ConnectionManagement {
    SetClientName(String), SelectDatabase(i32), Ping(String),
}

#[derive(Debug, PartialEq)]
pub enum ServerManagement {
    DbSize, Command(CommandOption), Info(Topic),
}

#[derive(Debug, PartialEq)]
pub enum Topic {
    Keyspace, Server, Named(String),
}

#[derive(Debug, PartialEq)]
pub enum Generic {
    Keys(String),
    Scan { cursor:  usize,
           pattern: Option<String>,
           count:   Option<usize>,
           tpe:     Option<String>, }
}

#[derive(Debug, PartialEq)]
pub enum CommandOption {
    Empty, Docs
}

#[derive(Debug, PartialEq)]
pub enum ListVerb {
    Length(String),
    Append(String, Vec<String>),
    Prepend(String, Vec<String>),
    Range(String, i32, i32),
}

#[derive(Debug, PartialEq)]
pub enum Command {
    ConnectionManagement(ConnectionManagement),
    ServerManagement(ServerManagement),
    Generic(Generic),
    Lists(ListVerb),
    Unknown(String),
}

impl Command {
    fn wrong_category<A>() -> Result<A, io::Error> {
        Err(io::Error::new(io::ErrorKind::InvalidInput, "Unknown or incomplete command."))
    }

    fn unknown(command: Message) -> Result<Self, io::Error> {
        match command.try_as_bulk_array().as_deref() {
            Some([unknown @ ..]) => Ok(Command::Unknown(unknown.join(" "))),
            _otherwise           => Self::wrong_category(),
        }
    }

    fn decode<A: str::FromStr>(image: &str) -> Result<A, io::Error> 
    where
        A::Err: fmt::Display
    {
        image.parse::<A>().map_err(
            |e: A::Err| io::Error::new(io::ErrorKind::InvalidInput, e.to_string())
        )
    }
}

impl TryFrom<Message> for Command {
    type Error = io::Error;
    fn try_from(command: Message) -> Result<Self, Self::Error> {
        println!("Command: {command}");
        ListVerb::try_from(command.clone()).map(Command::Lists)
            .or(ConnectionManagement::try_from(command.clone()).map(Command::ConnectionManagement))
            .or(ServerManagement::try_from(command.clone()).map(Command::ServerManagement))
            .or(Generic::try_from(command.clone()).map(Command::Generic))
            .or(Command::unknown(command))
    }
}

impl TryFrom<Message> for ConnectionManagement {
    type Error = io::Error;
    fn try_from(command: Message) -> Result<Self, Self::Error> {
        match command.try_as_bulk_array().as_deref() {
            Some(["CLIENT", "SETNAME", name]) => 
                Ok(ConnectionManagement::SetClientName(name.to_string())),
            Some(["PING", msg @ .. ]) => {
                let message = if msg.is_empty() {
                    "PONG".to_string()
                } else {
                    msg.join(" ")
                };
                Ok(ConnectionManagement::Ping(message))
            },
            Some(["SELECT", index]) =>
                Ok(ConnectionManagement::SelectDatabase(Command::decode(index)?)),
            _otherwise =>
                Command::wrong_category(),
        }
    }
}

impl TryFrom<Message> for ServerManagement {
    type Error = io::Error;
    fn try_from(command: Message) -> Result<Self, Self::Error> {
        match command.try_as_bulk_array().as_deref() {
            Some(["COMMAND", "DOCS"])  => Ok(ServerManagement::Command(CommandOption::Docs)),
            Some(["COMMAND"])          => Ok(ServerManagement::Command(CommandOption::Empty)),
            Some(["DBSIZE"])           => Ok(ServerManagement::DbSize),
            Some(["INFO", "keyspace"]) => Ok(ServerManagement::Info(Topic::Keyspace)),
            Some(["INFO", "server"])   => Ok(ServerManagement::Info(Topic::Server)),
            Some(["INFO", topic])      => Ok(ServerManagement::Info(Topic::Named(topic.to_string()))),
            _otherwise                 => Command::wrong_category(),
        }
    }
}

impl TryFrom<Message> for Generic {
    type Error = io::Error;

    fn try_from(command: Message) -> Result<Self, Self::Error> {
        match command.try_as_bulk_array().as_deref() {
            Some(["KEYS", pattern]) =>
                Ok(Generic::Keys(pattern.to_string())),
            Some(["SCAN", cursor]) =>
                Ok(Generic::Scan {
                    cursor: Command::decode(cursor)?, pattern: None, count: None, tpe: None
                }),
            _otherwise =>
                Command::wrong_category(),
        }
    }
}

impl TryFrom<Message> for ListVerb {
    type Error = io::Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        match value.try_as_bulk_array().as_deref() {
            Some(["LRANGE", key, start, stop]) =>
                Ok(ListVerb::Range(
                    key.to_string(), Command::decode(start)?, Command::decode(stop)?
                )),
            Some(["RPUSH", key, elements @ ..]) =>
                Ok(ListVerb::Append(
                    key.to_string(),
                    /* Is this really the correct way? */
                    elements.to_vec().iter().map(|s| s.to_string()).collect(),
                )),
            Some(["LPUSH", key, elements @ ..]) =>
                Ok(ListVerb::Prepend(
                    key.to_string(),
                    /* Is this really the correct way? */
                    elements.to_vec().iter().map(|s| s.to_string()).collect(),
                )),
            Some(["LLEN", key]) =>
                Ok(ListVerb::Length(key.to_string())),
            _otherwise =>
                Command::wrong_category(),
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn make_command(words: Vec<&str>) -> Message {
        Message::Array(
            words.iter().map(|s| Message::BulkString(s.to_string())).collect()
        )
    }

    #[test]
    fn lists() {
        assert_eq!(
            Command::try_from(make_command(vec!["LPUSH", "mylist", "Kalle"])).unwrap(),
            Command::Lists(ListVerb::Prepend("mylist".to_string(), vec!["Kalle".to_string()])),
        );
        assert_eq!(
            Command::try_from(make_command(vec!["LLEN", "mylist"])).unwrap(),
            Command::Lists(ListVerb::Length("mylist".to_string())),
        );
    }
}