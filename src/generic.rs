use std::io;
use crate::commands;
use crate::core;
use crate::resp;
use crate::resp::Message;

pub enum Scan {
    Complete(Vec<String>),
    Chunk(usize, Vec<String>),
}

impl Scan {
    const DEFAULT_CHUNK_SIZE: usize = 10;

    fn to_owned(xs: Vec<&str>) -> Vec<String> {
        xs.into_iter().map(|x| x.into()).collect()   
    }

    fn complete(content: Vec<&str>) -> Scan {
        Self::Complete(Self::to_owned(content))
    }

    fn chunk(offset: usize, content: Vec<&str>) -> Scan {
        Self::Chunk(offset, Self::to_owned(content))
    }
}

impl From<Scan> for Message {
    fn from(scan: Scan) -> Self {
        fn make_reply_message(cursor: usize, content: Vec<String>) -> Message {
            let content = Message::make_bulk_array(content.as_slice());
            Message::make_array(vec![Message::Integer(cursor as i64), content])
        }

        match scan {
            Scan::Complete(xs)      => make_reply_message(0, xs),
            Scan::Chunk(cursor, xs) => make_reply_message(cursor, xs),
        }
    }
}

pub trait Generic {
    fn keys(&self, pattern: &str) -> Vec<String>;
    fn scan(
        &self, 
        cursor: usize, 
        pattern: Option<&str>, 
        count: Option<usize>, 
        tpe: Option<&str>
    ) -> Scan;
}

impl Generic for core::PersistentState {
    fn keys(&self, _pattern: &str) -> Vec<String> {
        self.strings.keys()
            .chain(self.lists.keys())
            .filter_map(|s| /* Eval glob pattern. */ Some(s.to_string()))
            .collect()
    }

    fn scan(
        &self, 
        cursor: usize, 
        _pattern: Option<&str>, 
        count: Option<usize>,
        _tpe: Option<&str>
    ) -> Scan {
        let combined_size = self.strings.len() + self.lists.len();
        let count = count.unwrap_or(Scan::DEFAULT_CHUNK_SIZE);
        let content = self.strings.keys().chain(self.lists.keys())
                          .skip(cursor).take(count)
                          .filter_map(|s| /* Eval pattern glob. */ Some(s.as_str()))
                          .collect::<Vec<&str>>();
        if cursor + count > combined_size {
            Scan::complete(content)
        } else {
            Scan::chunk(cursor + count + 1, content)
        }
    }
}

pub fn apply(
    state: &core::State,
    command: commands::Generic,
)  -> Result<resp::Message, io::Error> {
    match command {
        commands::Generic::Keys(pattern) => 
            Ok(Message::make_bulk_array(
                state.for_reading()?.keys(&pattern).as_slice()
            )),
        commands::Generic::Scan { cursor, pattern, count, tpe } =>
            Ok(Message::from(
                state.for_reading()?
                     .scan(cursor, pattern.as_deref(), count, tpe.as_deref())
            )),
    }
}