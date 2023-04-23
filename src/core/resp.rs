use std::fmt::Display;
use std::str::FromStr;
use std::io::Error;
use std::fmt;
use arbitrary::Arbitrary;

#[derive(Arbitrary, Clone, Debug, PartialEq)]
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

impl Display for ErrorPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => write!(f, ""),
            Self::Err => write!(f, "ERR"),
            Self::Named(name) => write!(f, "{name}"),
        }
    }
}

impl From<ErrorPrefix> for String {
    fn from(value: ErrorPrefix) -> Self {
        match value {
            ErrorPrefix::Empty       => String::new(),
            ErrorPrefix::Err         => "ERR".to_string(),
            ErrorPrefix::Named(name) => name,
        }
    }
}

#[derive(Arbitrary, Clone, Debug, PartialEq)]
pub enum Message {
    SimpleString(String),
    Error { prefix: ErrorPrefix, message: String },
    Integer(i64),
    BulkString(String),
    Array(Vec<Message>),
    Nil,
}

impl Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::SimpleString(s) | Message::BulkString(s) => write!(f, "{s}"),
            Message::Error { prefix, message } => write!(f, "(error) {prefix} {message}."),
            Message::Integer(i) => write!(f, "{i}"),
            Message::Array(xs) => {
                write!(f, "Array({}", xs.len())?;
                for (x, i) in xs.iter().zip(0..) {
                    write!(f, "({i}){x},")?;
                }
                write!(f, ")")?;
                Ok(())  /* No other construct here? */
            },
            Message::Nil => write!(f, "(nul)"),
        }
    }
}

/* Type-alias String so that I can keep String for dumb purposes. */
impl From<Message> for String {
    fn from(value: Message) -> Self {
        match value {
            Message::SimpleString(text) => format!("+{text}\r\n"),
            Message::Error { prefix, message } =>
                /* Fix later. */
                format!("-{} {}\r\n", String::from(prefix), message),
            Message::Integer(i) => format!(":{i}\r\n"),
            Message::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            Message::Array(elements) => {
                let xs: Vec<String> = elements.into_iter().map(Into::into).collect();
                format!("*{}\r\n{}", xs.len(), xs.join(""))
            },
            Message::Nil => "$-1\r\n".to_string(),
        }
    }
}

impl FromStr for Message {
    type Err = Error;

    fn from_str(phrase: &str) -> Result<Self, Self::Err> {
        parser::parse_message_phrase(phrase)
    }
}

impl Message {
    pub fn make_array(xs: Vec<Message>) -> Self {
        Message::Array(xs)
    }

    fn make_bulk_string(size: i32, text: &str) -> Self {
        if size == -1 {
            Message::Nil
        } else {
            Message::BulkString(text.to_string())
        }
    }

    fn parse_error(line: &str) -> Self {
        if let Some(ix) = line.find(' ') {
            let (prefix, suffix) = line.split_at(ix);
            Message::Error {
                prefix: ErrorPrefix::make(prefix.trim()),
                message: suffix.trim().to_string(),
            }
        } else {
            Message::Error {
                prefix:  ErrorPrefix::Empty,
                message: line.trim().to_string()
            }
        }
    }

    pub fn make_bulk_array(xs: &[String]) -> Self {
        Message::make_array(
            xs.iter().cloned().map(Message::BulkString).collect()
        )
    }

    fn try_as_bulk_string_content(&self) -> Option<&str> {
        if let Message::BulkString(s) = self { Some(s) } else { None }
    }

    fn as_array_contents(&self) -> Option<&Vec<Message>> {
        match self {
            Message::Array(bs) => Some(bs),
            _ => None,
        }
    }

    pub fn try_as_bulk_array(&self) -> Option<Vec<&str>> {
        self.as_array_contents()?
            .iter()
            .map(Message::try_as_bulk_string_content)
            .collect()
    }
}

pub mod parser {
    use super::*;
    use std::io;
    use io::BufRead;

    fn end_of_file<A>() -> io::Result<A> {
        Err(Error::new(io::ErrorKind::UnexpectedEof, "end of file"))
    }

    pub fn read_message<S: BufRead>(reader: &mut S) -> io::Result<Message> {
        let mut buffer: Vec<Token> = vec![];
        let mut lines = reader.lines();
        loop {
            match lines.next() {
                Some(Ok(token_image)) => {
                    let token = Token::parse(&token_image);
                    buffer.push(token);

                    if let Some(message) = try_commit_prefix(&mut buffer) {
                        break Ok(message)
                    }
                },
                Some(Err(e)) => break Err(e),
                None         => break end_of_file(),
            }
        }
    }

    fn try_commit_prefix(buffer: &mut Vec<Token>) -> Option<Message> {
        match parser::parse_prefix(buffer) {
            (Ok(message), suffix) => {
                *buffer = suffix.to_vec();
                Some(message)
            },
            _ => None,
        }
    }

    #[derive(Clone, Debug)]
    pub enum Token {
        Literal(String),
        Trivial { parsed: Message, image: String, },
        BulkString { parsed: i32, image: String, },
        Array { parsed: i32, image: String, },
    }

    impl Token {
        fn raw_image(&self) -> &str {
            match self {
                Token::Literal(image)                  |
                Token::Trivial    { parsed: _, image } |
                Token::BulkString { parsed: _, image } |
                Token::Array      { parsed: _, image } => image,
            }
        }

        fn produce(prefix: &str, suffix: &str, token_image: &str) -> Token {
            /* The repetitions tickle my DRY nerves. Is this the way? */
            match prefix {
                "+" => Token::Trivial { 
                            parsed: Message::SimpleString(suffix.to_string()), 
                            image: token_image.to_string(),
                       },
                "-" => Token::Trivial { 
                            parsed: Message::parse_error(suffix),
                            image: token_image.to_string(),
                       },
                ":" => suffix.parse().map_or_else(
                            |_| Token::Literal(token_image.to_string()),
                            |v| Token::Trivial {
                                    parsed: Message::Integer(v),
                                    image: token_image.to_string(),
                                }
                       ),
                "*" => suffix.parse().map_or_else(
                            |_| Token::Literal(token_image.to_string()),
                            |v| Token::Array {
                                    parsed: v,
                                    image: token_image.to_string(),
                                }
                       ),
                "$" => suffix.parse().map_or_else(
                            |_| Token::Literal(token_image.to_string()),
                            |v| Token::BulkString {
                                    parsed: v,
                                    image: token_image.to_string(),
                                }
                       ),
                _   => Token::Literal(token_image.to_string()),
            }            
        }

        pub fn parse(line: &str) -> Token {
            if line.is_empty() {
                Token::Literal(String::new())
            } else {
                let prefix = &line[0..1];
                let suffix = &line[1..];
                Token::produce(prefix, suffix, line)                
            }
        }
    }

    /* What about this lifetime thing? */
    fn parse_array<'a>(
        count:  i32, 
        input:  &'a [Token],
        output: &mut Vec<Message>,
    ) -> &'a [Token] {
        if count == 0 {
            input
        } else {
            match parse_prefix(input) {
                (Ok(element), remaining) => {
                    output.push(element);
                    parse_array(count - 1, remaining, output)
                }
                _ => input,
            }
        }
    }

    pub fn parse_prefix(input: &[Token]) -> (Result<Message, Error>, &[Token]) {
        match input {
            [Token::Trivial { parsed, image: _ }, tail @ ..] =>
                (Ok(parsed. clone()), tail),
            [Token::BulkString { parsed: size, image: _ }, tail @ ..] if *size == -1 => 
                (Ok(Message::Nil), tail),
            [Token::BulkString { parsed: size, image: _ }, contents, tail @ ..] =>
                (Ok(Message::make_bulk_string(*size, contents.raw_image())), tail),
            [Token::Array { parsed: length, image: _ }, tail @ ..] if *length > -1 => {
                let requested_length = *length as usize;
                let mut elements = Vec::with_capacity(requested_length);
                let remaining = parse_array(*length, tail, &mut elements);

                if elements.len() == requested_length {
                    (Ok(Message::make_array(elements)), remaining)
                } else {
                    (Err(Error::new(io::ErrorKind::InvalidData, "Expected more array elements")), input)
                }
            },
            [Token::Array { parsed: _, image: _ }, tail @ ..] =>
                (Ok(Message::Nil), tail),
            _ => {
                let message = format!("Will not parse token stream: {input:?}");
                (Err(Error::new(io::ErrorKind::InvalidData, message)), input)
            },
        }
    }

    pub fn parse_message_phrase(phrase: &str) -> Result<Message, Error> {
        let tokens =
            phrase.split("\r\n")
                  .map(Token::parse)
                  .collect::<Vec<Token>>();
        parse_prefix(&tokens).0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_strings() {
        assert_eq!(
            "+OK\r\n".parse::<Message>().unwrap(),
            Message::SimpleString("OK".to_string()), 
        )
    }

    #[test]
    fn errors() {
        assert_eq!(
            "-Error message\r\n".parse::<Message>().unwrap(),
            Message::Error {
                prefix: ErrorPrefix::Named("Error".to_string()), 
                message: "message".to_string()
            }
        );
        assert_eq!(
            "-WRONGTYPE Operation against a key holding the wrong kind of value".parse::<Message>().unwrap(),
            Message::Error {
                prefix: ErrorPrefix::Named("WRONGTYPE".to_string()), 
                message: "Operation against a key holding the wrong kind of value".to_string()
            }
        );
        assert_eq!(
            "-ERR unknown command 'helloworld'".parse::<Message>().unwrap(),
            Message::Error {
                prefix: ErrorPrefix::Err,
                message: "unknown command 'helloworld'".to_string()
            }
        );
        assert_eq!(
            "-ERR".parse::<Message>().unwrap(),
            Message::Error { prefix: ErrorPrefix::Empty, message: "ERR".to_string() }
        )
    }

    #[test]
    fn integers() {
        assert_eq!(
            ":0\r\n".parse::<Message>().unwrap(),
            Message::Integer(0),
        );
        assert_eq!(
            ":1234130\r\n".parse::<Message>().unwrap(),
            Message::Integer(1234130),
        );
    }

    #[test]
    fn bulk_strings() {
        assert_eq!(
            "$5\r\nhello\r\n".parse::<Message>().unwrap(),
            Message::BulkString("hello".to_string()),
        );
        /* Fails from broken handling of BulkStrings. */
        assert_eq!(
            "$5\r\n$hell\r\n".parse::<Message>().unwrap(),
            Message::BulkString("$hell".to_string()),
        );
        assert_eq!(
            "$0\r\n\r\n".parse::<Message>().unwrap(),
            Message::BulkString("".to_string()),
        );
        assert_eq!(
            "$-1\r\n".parse::<Message>().unwrap(),
            Message::Nil,
        );
    }

    #[test]
    fn arrays() {
        assert_eq!(
            "*0\r\n".parse::<Message>().unwrap(),
            Message::Array(vec![]),
        );
        assert_eq!(
            "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".parse::<Message>().unwrap(),
            Message::Array(vec![
                Message::BulkString("hello".to_string()),
                Message::BulkString("world".to_string()),
            ]),
        );
        assert_eq!(
            "*3\r\n:1\r\n:2\r\n:3\r\n".parse::<Message>().unwrap(),
            Message::Array(vec![
                Message::Integer(1),
                Message::Integer(2),
                Message::Integer(3),
            ]),
        );
        assert_eq!(
            "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n".parse::<Message>().unwrap(),
            Message::Array(vec![
                Message::Integer(1),
                Message::Integer(2),
                Message::Integer(3),
                Message::Integer(4),
                Message::BulkString("hello".to_string()),
            ]),
        );
        assert_eq!(
            "*-1\r\n".parse::<Message>().unwrap(),
            Message::Nil,
        );
        assert_eq!(
            "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n".parse::<Message>().unwrap(),
            Message::Array(vec![
                Message::Array(vec![
                    Message::Integer(1),
                    Message::Integer(2),
                    Message::Integer(3),
                ]),
                Message::Array(vec![
                    Message::SimpleString("Hello".to_string()),
                    Message::Error { prefix: ErrorPrefix::Empty, message: "World".to_string() }
                ])                
            ]),
        );
        assert_eq!(
            "*3\r\n$5\r\nhello\r\n$-1\r\n$5\r\nworld\r\n".parse::<Message>().unwrap(),
            Message::Array(vec![
                Message::BulkString("hello".to_string()),
                Message::Nil,
                Message::BulkString("world".to_string()),
            ]),
        );
    }
}