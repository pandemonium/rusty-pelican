use std::fs;
use std::io;
use std::io::{BufRead, Write};
use std::path;
use std::iter;
use std::ops::{Deref, DerefMut};

use crate::resp;
use crate::resp::parser::{ParseState, Token};

pub trait WriteTransactionSink {
    fn record_write(&mut self, message: &resp::Message) -> Result<(), io::Error>;
}

impl <A> WriteTransactionSink for WithTransactionLog<A> {
    fn record_write(&mut self, message: &resp::Message) -> Result<(), io::Error> {
        self.log.append(message.clone())
    }
}

pub struct WithTransactionLog<Underlying> {
    log:        TransactionLog,
    underlying: Underlying,
}

impl <A> WithTransactionLog<A> {
    pub fn new(underlying: A) -> Result<Self, io::Error> {
        let default_path = path::Path::new("data/transactions.log");
        
        Ok(Self {
            log: TransactionLog::new(default_path)?,
            underlying: underlying,
        })
    }
}

impl <A> Deref for WithTransactionLog<A> {
    type Target = A;

    fn deref(&self) -> &Self::Target {
        &self.underlying
    }
}

impl <A> DerefMut for WithTransactionLog<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.underlying
    }
}

struct ReplayView(fs::File);

impl ReplayView {
    fn new(file: fs::File) -> Self {
        Self(file)
    }

    fn iter(&self) -> impl Iterator<Item = resp::Message> {
        let reader = io::BufReader::new(&self.0);
        let mut state = ParseState::empty();

        /* Can this be fused with from_fn somehow? */
        for line in reader.lines() {
            match line {
                Ok(x)  => state.add_token(Token::parse(&x)),
                Err(_e) => /* how to signal this? */ break,
            }
        }

        iter::from_fn(move || state.try_parse_message())
    }
}

struct TransactionLog {
    path: Box<path::Path>,
    aof: fs::File,
}

impl TransactionLog {
    fn new(at: &path::Path) -> Result<Self, io::Error> {
        Ok(Self {
            path: at.into(),
            aof:  fs::OpenOptions::new().append(true).create(true).open(at)?
        })
    }

    fn append(&mut self, message: resp::Message) -> Result<(), io::Error> {
        let record = String::from(message);
        self.aof.write_all(record.as_bytes())
        /* if now > fs_sync deadline { file.fs_sync() } */
    }

    fn sync(&self) -> Result<(), io::Error> {
        self.aof.sync_all()
    }

    fn replay(&self) -> Result<ReplayView, io::Error> {
        Ok(ReplayView::new(fs::File::open(&self.path)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::temp_dir;
    use std::iter;
    use arbitrary::{*, unstructured::ArbitraryIter};
    use rand::Rng;

    fn truncate(path: &path::Path) -> Result<(), io::Error> {
        fs::OpenOptions::new().write(true).create(true).truncate(true).open(path)?;
        Ok(())
    }

    #[test]
    fn end_to_end() {
        let path = temp_dir().with_file_name("transactions.log");
        truncate(&path).unwrap();

        let mut log = TransactionLog::new(&path).unwrap();
        log.append(resp::Message::BulkString("Hi, mom".to_string())).unwrap();
        log.append(resp::Message::Integer(427)).unwrap();

        let log = TransactionLog::new(&path).unwrap();
        assert_eq!(log.replay().unwrap().iter().collect::<Vec<resp::Message>>(), vec![
            resp::Message::BulkString("Hi, mom".to_string()),
            resp::Message::Integer(427)
        ])
    }

    #[test]
    fn arbitariness() {
        /* Does this even work? */
        let random_bytes = rand::thread_rng().gen::<[u8; 32]>();
        let mut u = Unstructured::new(&random_bytes);
        let ms = u.arbitrary::<Vec<resp::Message>>().unwrap();

        let path = temp_dir().with_file_name("transactions2.log");
        truncate(&path).unwrap();

        let mut log = TransactionLog::new(&path).unwrap();
        for m in ms.iter() {
            log.append(m.clone()).unwrap();
        }
        log.sync().unwrap();

        let log = TransactionLog::new(&path).unwrap();
        assert_eq!(log.replay().unwrap().iter().collect::<Vec<resp::Message>>(), ms);
    }
}