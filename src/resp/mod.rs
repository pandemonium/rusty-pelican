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

    pub fn read(source: &str) -> Option<Value> {
        parser::read_value(source)
    }
}

mod parser {
    use super::Value;

    enum Token {
        Literal(String),
        Trivial(Value),
        BulkString(i32),
        Array(i32),
    }

    impl Token {
        fn trivial(v: Value) -> Option<Token> {
            Some(Token::Trivial(v))
        }

        fn produce(prefix: &str, suffix: &str) -> Option<Token> {
            match prefix {
                "+"       => Token::trivial(Value::SimpleString(suffix.to_string())),
                "-"       => Token::trivial(Value::make_error(suffix)),
                ":"       => suffix.parse().ok().map(|v| Token::Trivial(Value::Integer(v))),
                "*"       => suffix.parse().ok().map(Token::Array),
                "$"       => suffix.parse().ok().map(Token::BulkString),
                otherwise => None,
            }
        }

        fn read(line: &str) -> Option<Token> {
            let head = &line[0..1];
            let tail = &line[1..];

            Token::produce(head, tail)
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
                (Some(element), remaining) => {
                    output.push(element);
                    parse_array(count - 1, remaining, output)
                }
                _ => input,
            }
        }
    }

    fn parse_value(input: &[Token]) -> (Option<Value>, &[Token]) {
        match input {
            [Token::Trivial(value), tail @ ..] =>
                (Some(value.clone()), tail),
            [Token::BulkString(size), Token::Literal(text), tail @ ..] => 
                (Some(Value::make_bulk_string(*size, text)), tail),
            [Token::Array(length), tail @ ..] => {
                let mut elements = Vec::with_capacity(*length as usize);
                let remaining = parse_array(*length, tail, &mut elements);
                (Some(Value::make_array(elements)), remaining)
            },
            diverged => 
                (None, input),
        }
    }

    pub fn read_value(text: &str) -> Option<Value> {
        text.split("\r\n")
            .map(Token::read)
            .collect::<Option<Vec<Token>>>()
            .and_then(|input| parse_value(input.as_slice()).0)
    }
}