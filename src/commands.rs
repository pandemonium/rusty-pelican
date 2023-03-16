use std::convert::TryFrom;
use std::io;
use std::num;
use crate::resp::*;


#[derive(Debug, PartialEq)]
pub enum Info {
    Server,
    Keyspace,
    Category(String),
}

#[derive(Debug, PartialEq)]
pub enum Client {
    SetName(String),
}

#[derive(Debug, PartialEq)]
pub enum Schema {
    Select(i32),
    Keys(String),
}

/* Find a name for this. */
#[derive(Debug, PartialEq)]
pub enum Miscellaneous {
    Empty,
    Docs,
    Ping(String),
    Schema(Schema),
    Unknown(String),
}

#[derive(Debug, PartialEq)]
pub enum List {
    Length(String),
    Append(String, Vec<String>),
    Prepend(String, Vec<String>),
    Range(String, i32, i32),
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Other(Miscellaneous),
    Client(Client),
    Info(Info),
    List(List),
}

impl TryFrom<Message> for Command {
    type Error = io::Error;

    fn try_from(command: Message) -> Result<Self, Self::Error> {
        println!("Command: {command}");

        List::try_from(command.clone()).map(Command::List)
             .or(Client::try_from(command.clone()).map(Command::Client))
             .or(Info::try_from(command.clone()).map(Command::Info))
             .or(Miscellaneous::try_from(command.clone()).map(Command::Other))
    }
}

impl TryFrom<Message> for Info {
    type Error = io::Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        match value.try_as_bulk_array().as_deref() {
            Some(["INFO", "keyspace"]) => Ok(Info::Keyspace),
            Some(["INFO", "server"]) => Ok(Info::Server),
            Some(["INFO", category]) => Ok(Info::Category(category.to_string())),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown or incomplete command.")),
        }
    }
}

impl TryFrom<Message> for Client {
    type Error = io::Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        match value.try_as_bulk_array().as_deref() {
            Some(["CLIENT", "SETNAME", name]) => Ok(Client::SetName(name.to_string())),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown or incomplete command.")),
        }
    }
}

impl TryFrom<Message> for List {
    type Error = io::Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        match value.try_as_bulk_array().as_deref() {
            Some(["LRANGE", key, start, stop]) => {
                let start = start.parse::<i32>().map_err(
                    |e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string())
                )?;
                let stop = stop.parse::<i32>().map_err(
                    |e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string())
                )?;
                Ok(List::Range(key.to_string(), start, stop))
            },
            Some(["RPUSH", key, elements @ ..]) =>
                Ok(List::Append(
                    key.to_string(),
                    /* Is this really the correct way? */
                    elements.to_vec().iter().map(|s| s.to_string()).collect(),
                )),
            Some(["LPUSH", key, elements @ ..]) =>
                Ok(List::Prepend(
                    key.to_string(),
                    /* Is this really the correct way? */
                    elements.to_vec().iter().map(|s| s.to_string()).collect(),
                )),
            Some(["LLEN", key]) =>
                Ok(List::Length(key.to_string())),
            _ =>
                Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown or incomplete command.")),
        }
    }
}

impl TryFrom<Message> for Miscellaneous {
    type Error = io::Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        match value.try_as_bulk_array().as_deref() {
            Some(["COMMAND", "DOCS"]) =>
                Ok(Miscellaneous::Docs),
            Some(["COMMAND"]) =>
                Ok(Miscellaneous::Empty),
            Some(["PING", msg @ .. ]) => {
                let message = if msg.is_empty() {
                    "PONG".to_string()
                } else {
                    msg.join(" ")
                };
                Ok(Miscellaneous::Ping(message))
            },
            Some(["SELECT", index]) => {
                let index = index.parse::<i32>().map_err(
                    |e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string())
                )?;
                Ok(Miscellaneous::Schema(Schema::Select(index)))
            },
            Some(["DBSIZE"]) =>
                Ok(Miscellaneous::Schema(Schema::Keys("*".to_string()))),
            Some(["KEYS", pattern]) =>
                Ok(Miscellaneous::Schema(Schema::Keys(pattern.to_string()))),
            Some([unknown @ ..]) =>
                Ok(Miscellaneous::Unknown(unknown.join(" "))),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown or incomplete command.")),
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
            Command::List(List::Prepend("mylist".to_string(), vec!["Kalle".to_string()])),
        );
        assert_eq!(
            Command::try_from(make_command(vec!["LLEN", "mylist"])).unwrap(),
            Command::List(List::Length("mylist".to_string())),
        );
    }
}