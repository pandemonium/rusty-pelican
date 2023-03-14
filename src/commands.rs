use std::convert::TryFrom;
use std::io::{Error, ErrorKind};
use crate::resp::*;


#[derive(Debug, PartialEq)]
pub enum Cmd {
    Docs,
}

#[derive(Debug, PartialEq)]
pub enum List {
    Length(String),
    Push(String, Vec<String>),
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Cmd(Cmd),
    List(List),
}

impl TryFrom<Message> for Command {
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        List::try_from(value.clone()).map(Command::List)
            .or(Cmd::try_from(value).map(Command::Cmd))
    }
}

impl TryFrom<Message> for List {
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        match value.try_as_bulk_array().as_deref() {
            Some(["LPUSH", key, elements @ ..]) =>
                Ok(List::Push(
                    key.to_string(), 
                    elements.to_vec().iter().map(|s| s.to_string()).collect(),
                )),
            Some(["LLEN", key]) =>
                Ok(List::Length(key.to_string())),
            _ => 
                Err(
                    Error::new(ErrorKind::InvalidData, "Unknown or incomplete command.")
                ),
        }
    }
}

impl TryFrom<Message> for Cmd {
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        match value.try_as_bulk_array().as_deref() {
            Some(["COMMAND", "DOCS"]) =>
                Ok(Cmd::Docs),
            _ =>
                Err(
                    Error::new(ErrorKind::InvalidData, "Unknown or incomplete command.")
                ),
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
            Command::List(List::Push("mylist".to_string(), vec!["Kalle".to_string()])),
        );
        assert_eq!(
            Command::try_from(make_command(vec!["LLEN", "mylist"])).unwrap(),
            Command::List(List::Length("mylist".to_string())),
        );
    }
}