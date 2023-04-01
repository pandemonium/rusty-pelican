use std::convert::TryFrom;
use std::io;
use std::fmt;
use std::str;

use crate::resp::*;
use crate::datatype::*;


#[derive(Clone, Debug, PartialEq)]
pub enum ConnectionManagement {
    SetClientName(String), SelectDatabase(i32), Ping(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ServerManagement {
    DbSize, Command(CommandOption), Info(Topic), BgSave,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Topic {
    Keyspace, Server, Named(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Generic {
    Ttl(String),
    Expire(String, u64),
    Keys(String),
    Scan { cursor:  usize,
           pattern: Option<String>,
           count:   Option<usize>,
           tpe:     Option<String>, },
    Exists(String),
    Type(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum CommandOption {
    Empty, Docs
}

#[derive(Clone, Debug, PartialEq)]
pub enum Command {
    ConnectionManagement(ConnectionManagement),
    ServerManagement(ServerManagement),
    Generic(Generic),
    Lists(lists::ListApi),
    Strings(keyvalues::StringsApi),
    SortedSets(sorted_sets::SortedSetApi),
    Unknown(String),
}

impl Command {
    fn wrong_category<A>() -> Result<A, io::Error> {
        Err(io::Error::new(io::ErrorKind::InvalidInput, "Unknown or incomplete command."))
    }

    fn unknown(command: &Message) -> Result<Self, io::Error> {
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

impl TryFrom<&Message> for Command {
    type Error = io::Error;
    fn try_from(command: &Message) -> Result<Self, Self::Error> {
        println!("Command: {command}");
        lists::ListApi::try_from(command.clone()).map(Command::Lists)
            .or_else(|_| keyvalues::StringsApi::try_from(command.clone()).map(Command::Strings))
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
            Some(["BGSAVE" | "bgsave"])                     => Ok(ServerManagement::BgSave),
            _otherwise                                      => Command::wrong_category(),
        }
    }
}

/* In generic.rs too? */
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
            Some(["SCAN" | "scan", cursor, "MATCH" | "match", pattern, "COUNT" | "count", count]) =>
                Ok(Generic::Scan {
                    cursor: Command::decode(cursor)?,
                    pattern: Some(pattern.to_string()),
                    count: Some(Command::decode(count)?),
                    tpe: None,
                }),
            Some(["TTL" | "ttl", key]) =>
                Ok(Generic::Ttl(key.to_string())),
            Some(["EXPIRE" | "expire", key, ttl]) =>
                Ok(Generic::Expire(key.to_string(), Command::decode(ttl)?)),
            Some(["EXISTS" | "exists", key]) =>
                Ok(Generic::Exists(key.to_string())),
            Some(["TYPE" | "type", key]) =>
                Ok(Generic::Type(key.to_string())),
            _otherwise =>
                Command::wrong_category(),
        }
    }
}

impl TryFrom<Message> for lists::ListApi {
    type Error = io::Error;
    fn try_from(value: Message) -> Result<Self, Self::Error> {
        match value.try_as_bulk_array().as_deref() {
            Some(["LRANGE" | "lrange", key, start, stop]) =>
                Ok(lists::ListApi::Range(
                    key.to_string(), Command::decode(start)?, Command::decode(stop)?
                )),
            Some(["RPUSH" | "rpush", key, elements @ ..]) =>
                Ok(lists::ListApi::Append(
                    key.to_string(),
                    elements.iter().map(|&s| s.into()).collect(),
                    false,
                )),
            Some(["RPUSHX" | "rpushx", key, elements @ ..]) =>
                Ok(lists::ListApi::Append(
                    key.to_string(),
                    elements.iter().map(|&s| s.into()).collect(),
                    true,
                )),
            Some(["LPUSH" | "lpush", key, elements @ ..]) =>
                Ok(lists::ListApi::Prepend(
                    key.to_string(),
                    elements.iter().map(|&s| s.into()).collect(),
                    false,
                )),
            Some(["LPUSHX" | "lpushx", key, elements @ ..]) =>
                Ok(lists::ListApi::Prepend(
                    key.to_string(),
                    elements.iter().map(|&s| s.into()).collect(),
                    true,
                )),
            Some(["LLEN" | "llen", key]) =>
                Ok(lists::ListApi::Length(key.to_string())),
            Some(["LSET" | "lset", key, index, element]) =>
                Ok(lists::ListApi::Set(
                    key.to_string(), 
                    Command::decode(index)?,
                    element.to_string(),
                )),
            _otherwise =>
                Command::wrong_category(),
        }
    }
}

impl TryFrom<Message> for keyvalues::StringsApi {
    type Error = io::Error;
    fn try_from(command: Message) -> Result<Self, Self::Error> {
        match command.try_as_bulk_array().as_deref() {
            Some(["SET" | "set", key, value]) =>
                Ok(keyvalues::StringsApi::Set(key.to_string(), value.to_string())),
            Some(["GET" | "get", key]) =>
                Ok(keyvalues::StringsApi::Get(key.to_string())),
            Some(["MGET" | "mget", keys @ ..]) =>
                Ok(keyvalues::StringsApi::Mget(keys.iter().map(|&s| s.into()).collect())),
            _otherwise =>
                Command::wrong_category(),
        }
    }
}

impl TryFrom<Message> for sorted_sets::SortedSetApi {
    type Error = io::Error;
    fn try_from(command: Message) -> Result<Self, Self::Error> {
        match command.try_as_bulk_array().as_deref() {
            Some(["ZADD" | "zadd", key, args @ ..]) => {
                let (options, entries) = sorted_sets::AddOptions::parse(args);
                let entries = entries.windows(2).map(|pär| {
                    match pär {
                        [score, member] =>
                            Command::decode(score).map(|score: f64| (score, member.to_string())),
                        bad_company =>
                            Err(io::Error::new(io::ErrorKind::InvalidInput, format!("bad format {:?}", bad_company))),
                    }
                }).collect::<Result<Vec<_>, Self::Error>>()?;

                Ok(sorted_sets::SortedSetApi::Add { key: key.to_string(), entries, options, })
            }
            Some(["ZRANGE" | "zrange", key, start, stop, "BYSCORE" | "byscore"]) => {
                Ok(sorted_sets::SortedSetApi::RangeByScore(
                    key.to_string(), Command::decode(start)?, Command::decode(stop)?
                ))
            }
            Some(["ZRANGE" | "zrange", key, start, stop]) => {
                Ok(sorted_sets::SortedSetApi::RangeByRank(
                    key.to_string(), Command::decode(start)?, Command::decode(stop)?
                ))
            }
            Some(["ZRANK" | "zrank", key, member]) => {
                Ok(sorted_sets::SortedSetApi::Rank(key.to_string(), member.to_string()))
            }
            Some(["ZSCORE" | "zscore", key, member]) => {
                Ok(sorted_sets::SortedSetApi::Score(key.to_string(), member.to_string()))
            }
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
            words.iter().map(|&s| Message::BulkString(s.into())).collect()
        )
    }

    #[test]
    fn lists() {
        assert_eq!(
            Command::try_from(&make_command(vec!["LPUSH", "mylist", "Kalle"])).unwrap(),
            Command::Lists(lists::ListApi::Prepend("mylist".to_string(), vec!["Kalle".to_string()], false)),
        );
        assert_eq!(
            Command::try_from(&make_command(vec!["LPUSHX", "mylist", "Kalle"])).unwrap(),
            Command::Lists(lists::ListApi::Prepend("mylist".to_string(), vec!["Kalle".to_string()], true)),
        );
        assert_eq!(
            Command::try_from(&make_command(vec!["RPUSH", "mylist", "Kalle"])).unwrap(),
            Command::Lists(lists::ListApi::Append("mylist".to_string(), vec!["Kalle".to_string()], false)),
        );
        assert_eq!(
            Command::try_from(&make_command(vec!["RPUSHX", "mylist", "Kalle"])).unwrap(),
            Command::Lists(lists::ListApi::Append("mylist".to_string(), vec!["Kalle".to_string()], true)),
        );
        assert_eq!(
            Command::try_from(&make_command(vec!["LLEN", "mylist"])).unwrap(),
            Command::Lists(lists::ListApi::Length("mylist".to_string())),
        );
    }
}