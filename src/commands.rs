use std::convert::TryFrom;
use std::io::{Error, ErrorKind};
use crate::resp::*;


/* Rename to system command? */
#[derive(Debug, PartialEq)]
pub enum Introspection {
    Empty,
    Docs,
}

#[derive(Debug, PartialEq)]
pub enum List {
    Length(String),
    Prepend(String, Vec<String>),
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Introspection(Introspection),
    List(List),
}

impl TryFrom<Message> for Command {
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        List::try_from(value.clone()).map(Command::List)
            .or(Introspection::try_from(value).map(Command::Introspection))
    }
}

impl TryFrom<Message> for List {
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        match value.try_as_bulk_array().as_deref() {
            Some(["LPUSH", key, elements @ ..]) =>
                Ok(List::Prepend(
                    key.to_string(),
                    /* Is this really the correct way? */
                    elements.to_vec().iter().map(|s| s.to_string()).collect(),
                )),
            Some(["LLEN", key]) =>
                Ok(List::Length(key.to_string())),
            _ => 
                Err(Error::new(ErrorKind::InvalidData, "Unknown or incomplete command.")),
        }
    }
}

impl TryFrom<Message> for Introspection {
    type Error = Error;

    fn try_from(value: Message) -> Result<Self, Self::Error> {
        match value.try_as_bulk_array().as_deref() {
            Some(["COMMAND", "DOCS"]) =>
                Ok(Introspection::Docs),
            Some(["COMMAND"]) =>
                Ok(Introspection::Empty),
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
            Command::List(List::Prepend("mylist".to_string(), vec!["Kalle".to_string()])),
        );
        assert_eq!(
            Command::try_from(make_command(vec!["LLEN", "mylist"])).unwrap(),
            Command::List(List::Length("mylist".to_string())),
        );
    }
}