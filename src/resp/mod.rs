use std::str::FromStr;
use std::io::{Error};

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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
        if xs.is_empty() {
            Value::Nil
        } else {
            Value::Array(xs)
        }
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