use std::str::FromStr;
use std::io::{Error};

#[derive(Clone, Debug, PartialEq)]
pub enum ErrorPrefix {
    Empty, Err,
    Named(String),
}

impl ErrorPrefix {
    fn make(prefix: &str) -> ErrorPrefix {
        match prefix {
            "ERR"     => ErrorPrefix::Err,
            otherwise => ErrorPrefix::Named(otherwise.to_string())
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    SimpleString(String),
    Error { prefix: ErrorPrefix, message: String },
    Integer(i64),
    BulkString(String),
    Array(Vec<Value>),
    Nil,
}

impl FromStr for Value {
    type Err = Error;

    fn from_str(phrase: &str) -> Result<Self, Self::Err> {
        parser::parse_value_phrase(phrase)
    }
}

impl Value {
    fn make_array(xs: Vec<Value>) -> Self {
        Value::Array(xs)
    }

    fn make_bulk_string(size: i32, text: &str) -> Self {
        if size == -1 {
            Value::Nil
        } else {
            Value::BulkString(text.to_string())
        }
    }

    fn make_error(line: &str) -> Self {
        if let Some(ix) = line.find(' ') {
            let (prefix, suffix) = line.split_at(ix);
            Value::Error {
                prefix: ErrorPrefix::make(prefix.trim()),
                message: suffix.trim().to_string(),
            }
        } else {
            Value::Error {
                prefix:  ErrorPrefix::Empty,
                message: line.trim().to_string()
            }
        }
    }

    fn make_bulk_array(xs: &Vec<&str>) -> Self {
        Value::make_array(
            xs.into_iter()
              .map(|x| Value::BulkString(x.to_string()))
              .collect()            
        )
    }

    fn try_as_bulk_string_content(&self) -> Option<&str> {
        match self {
            Value::BulkString(s) => Some(s),
            _ => None,
        }
    }

    fn as_array_contents(&self) -> Option<&Vec<Value>> {
        match self {
            Value::Array(bs) => Some(bs),
            _ => None,
        }
    }

    pub fn try_as_bulk_array(&self) -> Option<Vec<&str>> {
        self.as_array_contents()?
            .into_iter()
            .map(|v| v.try_as_bulk_string_content())
            .collect()
    }
}

mod parser {
    use super::Value;
    use std::num::ParseIntError;
    use std::str::FromStr;
    use std::io::{Error, ErrorKind};

    #[derive(Debug)]
    enum Token {
        Literal(String),
        Trivial(Value),
        BulkString(i32),
        Array(i32),
    }

    impl Token {
        fn trivial(v: Value) -> Result<Token, Error> {
            Ok(Token::Trivial(v))
        }

        fn produce(prefix: &str, suffix: &str) -> Result<Token, Error> {
            fn as_invalid_input(e: ParseIntError) -> Error {
                Error::new(ErrorKind::InvalidInput, e.to_string())
            }

            /* This isn't very good because it'll always "parse". Some
               lines are the BulkString data. */
            match prefix {
                "+" => Token::trivial(Value::SimpleString(suffix.to_string())),
                "-" => Token::trivial(Value::make_error(suffix)),
                ":" => suffix.parse().map(|v| Token::Trivial(Value::Integer(v)))
                             .map_err(as_invalid_input),
                "*" => suffix.parse().map(Token::Array)
                             .map_err(as_invalid_input),
                "$" => suffix.parse().map(Token::BulkString)
                             .map_err(as_invalid_input),
                _   => Ok(Token::Literal(format!("{}{}", prefix, suffix).to_string())),
            }            
        }

        fn read(line: &str) -> Result<Token, Error> {
            if line.len() > 0 {
                let prefix = &line[0..1];
                let suffix = &line[1..];
                Token::produce(prefix, suffix)
            } else {
                Ok(Token::Literal("".to_string()))
            }
        }
    }

    impl FromStr for Token {
        type Err = Error;

        fn from_str(image: &str) -> Result<Self, Self::Err> {
            Token::read(image)
        }
    }

    /* Should these functions be in impl Value? */
    fn parse_array<'a>(
        count:  i32, 
        input:  &'a [Token],
        output: &mut Vec<Value>,
    ) -> &'a [Token] {
        if count == 0 {
            input
        } else {
            match parse_value(input) {
                (Ok(element), remaining) => {
                    output.push(element);
                    parse_array(count - 1, remaining, output)
                }
                _ => input,
            }
        }
    }

    fn parse_value(input: &[Token]) -> (Result<Value, Error>, &[Token]) {
        match input {
            [Token::Trivial(value), tail @ ..] =>
                (Ok(value.clone()), tail),
            [Token::BulkString(size), Token::Literal(text), tail @ ..] => 
                (Ok(Value::make_bulk_string(*size, text)), tail),
            [Token::BulkString(size), tail @ ..] if *size == -1 => 
                (Ok(Value::Nil), tail),
            [Token::Array(length), tail @ ..] if *length > -1 => {
                let mut elements = Vec::with_capacity(*length as usize);
                let remaining = parse_array(*length, tail, &mut elements);
                (Ok(Value::make_array(elements)), remaining)
            },
            [Token::Array(_), tail @ ..] =>
                (Ok(Value::Nil), tail),
            _ => {
                let message = format!("Will not parse token stream: {:?}", input);
                (Err(Error::new(ErrorKind::InvalidData, message)), input)
            },
        }
    }

    pub fn parse_value_phrase(phrase: &str) -> Result<Value, Error> {
        phrase.split("\r\n")
              .map(|s| s.parse())
              .collect::<Result<Vec<Token>, Error>>()
              .and_then(|input| parse_value(input.as_slice()).0)  /* Is it a failure if there's text left? */
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_string() {
        assert_eq!(
            "+OK\r\n".parse::<Value>().unwrap(),
            Value::SimpleString("OK".to_string()), 
        )
    }

    #[test]
    fn errors() {
        assert_eq!(
            "-Error message\r\n".parse::<Value>().unwrap(),
            Value::Error {
                prefix: ErrorPrefix::Named("Error".to_string()), 
                message: "message".to_string()
            }
        );
        assert_eq!(
            "-WRONGTYPE Operation against a key holding the wrong kind of value".parse::<Value>().unwrap(),
            Value::Error {
                prefix: ErrorPrefix::Named("WRONGTYPE".to_string()), 
                message: "Operation against a key holding the wrong kind of value".to_string()
            }
        );
        assert_eq!(
            "-ERR unknown command 'helloworld'".parse::<Value>().unwrap(),
            Value::Error {
                prefix: ErrorPrefix::Err,
                message: "unknown command 'helloworld'".to_string()
            }
        );
        assert_eq!(
            "-ERR".parse::<Value>().unwrap(),
            Value::Error { prefix: ErrorPrefix::Empty, message: "ERR".to_string() }
        )
    }

    #[test]
    fn integers() {
        assert_eq!(
            ":0\r\n".parse::<Value>().unwrap(),
            Value::Integer(0),
        );
        assert_eq!(
            ":1234130\r\n".parse::<Value>().unwrap(),
            Value::Integer(1234130),
        );
    }

    #[test]
    fn bulk_strings() {
        assert_eq!(
            "$5\r\nhello\r\n".parse::<Value>().unwrap(),
            Value::BulkString("hello".to_string()),
        );
        /* Fails from broken handling of BulkStrings. */
        assert_eq!(
            "$5\r\n$hell\r\n".parse::<Value>().unwrap(),
            Value::BulkString("$hell".to_string()),
        );
        assert_eq!(
            "$0\r\n\r\n".parse::<Value>().unwrap(),
            Value::BulkString("".to_string()),
        );
        assert_eq!(
            "$-1\r\n".parse::<Value>().unwrap(),
            Value::Nil,
        );
    }

    #[test]
    fn arrays() {
        assert_eq!(
            "*0\r\n".parse::<Value>().unwrap(),
            Value::Array(vec![]),
        );
        assert_eq!(
            "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".parse::<Value>().unwrap(),
            Value::Array(vec![
                Value::BulkString("hello".to_string()),
                Value::BulkString("world".to_string()),
            ]),
        );
        assert_eq!(
            "*3\r\n:1\r\n:2\r\n:3\r\n".parse::<Value>().unwrap(),
            Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
            ]),
        );
        assert_eq!(
            "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n".parse::<Value>().unwrap(),
            Value::Array(vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
                Value::Integer(4),
                Value::BulkString("hello".to_string()),
            ]),
        );
        assert_eq!(
            "*-1\r\n".parse::<Value>().unwrap(),
            Value::Nil,
        );
        assert_eq!(
            "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n".parse::<Value>().unwrap(),
            Value::Array(vec![
                Value::Array(vec![
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Integer(3),
                ]),
                Value::Array(vec![
                    Value::SimpleString("Hello".to_string()),
                    Value::Error { prefix: ErrorPrefix::Empty, message: "World".to_string() }
                ])                
            ]),
        );
        assert_eq!(
            "*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n".parse::<Value>().unwrap(),
            Value::Array(vec![
                Value::BulkString("hello".to_string()),
                Value::Nil,
                Value::BulkString("world".to_string()),
            ]),
        );
    }
}