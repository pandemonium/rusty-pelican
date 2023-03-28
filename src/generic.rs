use std::io;
use crate::commands;
use crate::core;
use crate::resp;
use std::time;
use crate::resp::Message;

pub enum ScanResult {
    Complete(Vec<String>),
    Chunk(usize, Vec<String>),
}

impl ScanResult {
    const DEFAULT_CHUNK_SIZE: usize = 10;

    fn to_owned(xs: Vec<&str>) -> Vec<String> {
        xs.into_iter().map(|x| x.into()).collect()   
    }

    fn complete(content: Vec<&str>) -> ScanResult {
        Self::Complete(Self::to_owned(content))
    }

    fn chunk(offset: usize, content: Vec<&str>) -> ScanResult {
        Self::Chunk(offset, Self::to_owned(content))
    }
}

impl From<ScanResult> for Message {
    fn from(scan: ScanResult) -> Self {
        fn make_reply_message(cursor: usize, content: Vec<String>) -> Message {
            let content = Message::make_bulk_array(content.as_slice());
            Message::make_array(vec![Message::Integer(cursor as i64), content])
        }

        match scan {
            ScanResult::Complete(xs)      => make_reply_message(0, xs),
            ScanResult::Chunk(cursor, xs) => make_reply_message(cursor, xs),
        }
    }
}
pub enum Ttl {
    UnknownKey,
    Eternal,
    ExpiresIn(time::Duration),
}

pub trait Generic {
    fn get_ttl(&self, key: &str) -> Ttl;
    fn filter_keys(&self, pattern: &str) -> Vec<String>;
    fn scan_keys(
        &self, 
        cursor: usize, 
        pattern: Option<&str>, 
        count: Option<usize>, 
        tpe: Option<&str>
    ) -> ScanResult;
    fn type_of_key(&self, key: &str) -> Option<String>;
    fn key_exists(&self, key: &str) -> bool;
}

impl Generic for core::Domain {
    fn get_ttl(&self, key: &str) -> Ttl {
        let now = time::Instant::now();
        match self.ttl_remaining(&key.to_string(), &now) {
            Some(ttl)                         => Ttl::ExpiresIn(ttl),
            None if self.filter_keys(key).is_empty() => Ttl::UnknownKey,
            None                              => Ttl::Eternal,
        }
    }

    fn filter_keys(&self, _pattern: &str) -> Vec<String> {
        self.strings.keys()
            .chain(self.lists.keys())
            .filter_map(|s| /* FIXME! Eval glob pattern. */ Some(s.to_string()))
            .collect()
    }

    fn scan_keys(
        &self, 
        cursor: usize, 
        _pattern: Option<&str>, 
        count: Option<usize>,
        _tpe: Option<&str>
    ) -> ScanResult {
        let combined_size = self.strings.len() + self.lists.len();
        let count = count.unwrap_or(ScanResult::DEFAULT_CHUNK_SIZE);
        let content = self.strings.keys().chain(self.lists.keys())
                          .skip(cursor).take(count)
                          .filter_map(|s| /* Eval pattern glob. */ Some(s.as_str()))
                          .collect::<Vec<&str>>();
        if cursor + count > combined_size {
            ScanResult::complete(content)
        } else {
            ScanResult::chunk(cursor + count + 1, content)
        }
    }

    fn type_of_key(&self, key: &str) -> Option<String> {
        if self.strings.contains_key(key) {
            Some("string".to_string())
        } else if self.lists.contains_key(key) {
            Some("list".to_string())
        } else {
            None
        }
    }

    fn key_exists(&self, key: &str) -> bool {
        self.strings.keys()
            .chain(self.lists.keys())
            .any(|k| *k == key)
    }
}

impl From<Ttl> for Message {
    fn from(value: Ttl) -> Self {
        match value {
            Ttl::UnknownKey     => Message::Integer(-2),
            Ttl::Eternal        => Message::Integer(-1),
            Ttl::ExpiresIn(ttl) => Message::Integer(ttl.as_secs() as i64),
        }
    }
}

pub fn apply(
    state: &core::DomainContext,
    command: core::CommandContext<commands::Generic>,
)  -> Result<resp::Message, io::Error> {
    match &*command {
        commands::Generic::Keys(pattern) => 
            Ok(Message::make_bulk_array(
                state.for_reading()?.filter_keys(pattern).as_slice()
            )),
        commands::Generic::Scan { cursor, pattern, count, tpe } =>
            Ok(Message::from(
                state.for_reading()?
                     .scan_keys(*cursor, pattern.as_deref(), *count, tpe.as_deref())
            )),
        commands::Generic::Ttl(key) =>
            Ok(Message::from(
                state.for_reading()?.get_ttl(key)
            )),
        commands::Generic::Expire(key, ttl) => {
            /* There are return values here. 1 for set, 0 for non-existant key. */
            state.apply_transaction(&command, |data| {
                data.register_ttl(
                    &key.to_string(), 
                    time::Instant::now(), 
                    time::Duration::from_secs(*ttl)
                );
                Message::Integer(1)
            })
        },
        commands::Generic::Exists(key) =>
            Ok(Message::Integer(
                if state.for_reading()?.key_exists(&key.to_string()) {
                    1
                } else {
                    0
                }
            )),
        commands::Generic::Type(key) =>
            Ok(Message::SimpleString(
                state.for_reading()?
                     .type_of_key(&key.to_string())
                     .unwrap_or("none".to_string())
            )),
    }
}