use std::convert::TryFrom;
use std::io::Error;
use crate::resp::*;


#[derive(Debug, PartialEq)]
pub enum List {
    Length(String),
    Push(String, Vec<String>),
}

#[derive(Debug, PartialEq)]
pub enum Command {
    List(List),
}

impl TryFrom<Value> for Command {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        List::try_from(value).map(Command::List)
    }
}

impl TryFrom<Value> for List {
    type Error = Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value.try_as_bulk_array().as_deref() {
            Some(["LPUSH", key, elements @ ..]) =>
                Ok(List::Push(
                    key.to_string(), 
                    elements.to_vec().iter().map(|s| s.to_string()).collect(),
                )),
            Some(["LLEN", key]) =>
                Ok(List::Length(key.to_string())),
            _ => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_command(words: Vec<&str>) -> Value {
        Value::Array(
            words.iter().map(|s| Value::BulkString(s.to_string())).collect()
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