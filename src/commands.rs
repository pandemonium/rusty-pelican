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
    Ttl(String),
    Expire(String, u64),
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
pub enum ListApi {
    Length(String),
    Append(String, Vec<String>),
    Prepend(String, Vec<String>),
    Range(String, i32, i32),
}

#[derive(Debug, PartialEq)]
pub enum StringsApi {
    Set(String, String),
    Get(String),
    Mget(Vec<String>),
}

#[derive(Debug, PartialEq)]
pub enum Command {
    ConnectionManagement(ConnectionManagement),
    ServerManagement(ServerManagement),
    Generic(Generic),
    Lists(ListApi),
    Strings(StringsApi),
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
        ListApi::try_from(command.clone()).map(Command::Lists)
            .or_else(|_| StringsApi::try_from(command.clone()).map(Command::Strings))
            .or_else(|_| ConnectionManagement::try_from(command.clone()).map(Command::ConnectionManagement))
            .or_else(|_| ServerManagement::try_from(command.clone()).map(Command::ServerManagement))
            .or_else(|_| Generic::try_from(command.clone()).map(Command::Generic))
            .or_else(|_| Command::unknown(command))
    }
}

impl TryFrom<Message> for ConnectionManagement {
    type Error = io::Error;
    fn try_from(command: Message) -> Result<Self, Self::Error> {
        match command.try_as_bulk_array().as_deref() {
            Some(["CLIENT" | "client", "SETNAME" | "setname", name]) => 
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
            Some(["COMMAND" | "commands", "DOCS" | "docs"]) => Ok(ServerManagement::Command(CommandOption::Docs)),
            Some(["COMMAND" | "commands"])                  => Ok(ServerManagement::Command(CommandOption::Empty)),
            Some(["DBSIZE" | "dbsize"])                     => Ok(ServerManagement::DbSize),
            Some(["INFO" | "info", "keyspace"])             => Ok(ServerManagement::Info(Topic::Keyspace)),
            Some(["INFO" | "info", "server"])               => Ok(ServerManagement::Info(Topic::Server)),
            Some(["INFO" | "info", topic])                  => Ok(ServerManagement::Info(Topic::Named(topic.to_string()))),
            Some(["INFO" | "info"])                         => Ok(ServerManagement::Info(Topic::Named("topic.to_string()".to_string()))),
            _otherwise                                      => Command::wrong_category(),
        }
    }
}

impl TryFrom<Message> for Generic {
    type Error = io::Error;
    fn try_from(command: Message) -> Result<Self, Self::Error> {
        match command.try_as_bulk_array().as_deref() {
            Some(["KEYS" | "keys", pattern]) =>
                Ok(Generic::Keys(pattern.to_string())),
            Some(["SCAN" | "scan", cursor]) =>
                Ok(Generic::Scan {
                    cursor: Command::decode(cursor)?, pattern: None, count: None, tpe: None
                }),
            Some(["SCAN" | "scan", cursor, "COUNT" | "count", count]) =>
                Ok(Generic::Scan {
                    cursor: Command::decode(cursor)?, 
                    pattern: None, 
                    count: Some(Command::decode(count)?),
                    tpe: None,
                }),
            Some(["TTL" | "ttl", key]) =>
                Ok(Generic::Ttl(key.to_string())),
            Some(["EXPIRE" | "expire", key, ttl]) =>
                Ok(Generic::Expire(key.to_string(), Command::decode(ttl)?)),
            _otherwise =>
                Command::wrong_category(),
        }
    }
}

impl TryFrom<Message> for ListApi {
    type Error = io::Error;
    fn try_from(value: Message) -> Result<Self, Self::Error> {
        match value.try_as_bulk_array().as_deref() {
            Some(["LRANGE" | "lrange", key, start, stop]) =>
                Ok(ListApi::Range(
                    key.to_string(), Command::decode(start)?, Command::decode(stop)?
                )),
            Some(["RPUSH" | "rpush", key, elements @ ..]) =>
                Ok(ListApi::Append(
                    key.to_string(),
                    /* Is this really the correct way? */
                    elements.to_vec().iter().map(|s| s.to_string()).collect(),
                )),
            Some(["LPUSH" | "lpush", key, elements @ ..]) =>
                Ok(ListApi::Prepend(
                    key.to_string(),
                    /* Is this really the correct way? */
                    elements.to_vec().iter().map(|s| s.to_string()).collect(),
                )),
            Some(["LLEN" | "llen", key]) =>
                Ok(ListApi::Length(key.to_string())),
            _otherwise =>
                Command::wrong_category(),
        }
    }
}

impl TryFrom<Message> for StringsApi {
    type Error = io::Error;
    fn try_from(command: Message) -> Result<Self, Self::Error> {
        match command.try_as_bulk_array().as_deref() {
            Some(["SET" | "set", key, value]) =>
                Ok(StringsApi::Set(key.to_string(), value.to_string())),
            Some(["GET" | "get", key]) =>
                Ok(StringsApi::Get(key.to_string())),
            Some(["MGET" | "mget", keys @ ..]) =>
                Ok(StringsApi::Mget(keys.to_vec().iter().map(|s| s.to_string()).collect())),
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
            Command::Lists(ListApi::Prepend("mylist".to_string(), vec!["Kalle".to_string()])),
        );
        assert_eq!(
            Command::try_from(make_command(vec!["LLEN", "mylist"])).unwrap(),
            Command::Lists(ListApi::Length("mylist".to_string())),
        );
    }
}