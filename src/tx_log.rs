use std::fs;
use std::io;
use std::io::{BufRead, Write};
use std::path;
use std::ops::{Deref, DerefMut};
use std::time;
use serde::{Deserialize, Serialize};
use base64::{
    Engine as _, 
    engine::general_purpose::STANDARD_NO_PAD as base64_codec
};

use crate::resp;

#[derive(Clone, Serialize, Deserialize)]
pub struct Revision(usize);

impl Revision {
    pub fn succeeding(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl Default for Revision {
    fn default() -> Self {
        Self(Default::default())
    }
}

/* What is a good format for this file? Lines of Base 64-encoded bincode entries?
    Requirements:
    - Be able to append LogEntry:s as they come, stored as records;
    - Read records as a stream so that the entire file isn't needed in memory.
*/
#[derive(Serialize, Deserialize)]
struct LogEntry {
    at:       time::SystemTime,
    revision: Revision,
    content:  String,
}

impl LogEntry {
    fn new(at: time::SystemTime, revision: &Revision, message: &resp::Message) -> Self {
        Self {
            at, 
            revision: revision.clone(),
            content: message.clone().into(),
        }
    }
}

pub trait WriteTransactionSink {
    fn record_evidence(
        &mut self, 
        revision: &Revision, 
        message:  &resp::Message
    ) -> Result<(), io::Error>;
}

impl <A> WriteTransactionSink for LoggedTransactions<A> {
    fn record_evidence(
        &mut self,
        revision: &Revision,
        message:  &resp::Message
    ) -> Result<(), io::Error> {
        if !self.replaying {
            println!("record_write: appending to transaction log");
            let entry = LogEntry::new(time::SystemTime::now(), revision, message);
            self.log.append(entry)
        } else {
            Ok(println!("record_write: ignoring"))
        }
    }
}

pub struct LoggedTransactions<Wrapped> {
    log:        LogFile,
    underlying: Wrapped,
    replaying:  bool,
}

impl <Wrapped> LoggedTransactions<Wrapped> {
    pub fn new(underlying: Wrapped) -> Result<Self, io::Error> {
        let default_path = path::Path::new("data/transactions.log");

        Ok(Self {
            log:        LogFile::new(default_path)?,
            underlying: underlying,
            replaying:  true,
        })
    }

    pub fn transaction_log(&self) -> &LogFile {
        &self.log
    }

    pub fn finalize_replay(&mut self) {
        self.replaying = false;
    }
}

impl <A> Deref for LoggedTransactions<A> {
    type Target = A;

    fn deref(&self) -> &Self::Target {
        &self.underlying
    }
}

impl <A> DerefMut for LoggedTransactions<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.underlying
    }
}

pub struct ReplayView(fs::File);

impl ReplayView {
    fn new(file: fs::File) -> Self {
        Self(file)
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<resp::Message, io::Error>> + '_ {
        let reader = io::BufReader::new(&self.0);

        reader.lines().into_iter().map(|line| {
            line.and_then(|record| {
                let entry = LogFile::try_deserialize_entry(&record)?;
                entry.content.parse()
            })
        })
    }
}

pub struct LogFile {
    path: Box<path::Path>,  /* Why does this need a Box? */
    file: fs::File,
}

impl LogFile {
    fn new(at: &path::Path) -> Result<Self, io::Error> {
        Ok(Self {
            path: at.into(),
            file: fs::OpenOptions::new().append(true).create(true).open(at)?,
        })
    }

    fn append(&mut self, entry: LogEntry) -> Result<(), io::Error> {
        let record = Self::try_serialize_entry(&entry)?;
        self.file.write_all(format!("{record}\r\n").as_bytes())
        /* if now > fs_sync deadline { file.fs_sync() } */
    }

    fn try_serialize_entry(entry: &LogEntry) -> Result<String, io::Error> { 
        let data = bincode::serialize(&entry).unwrap(); /* De-unwrap:ify. */
        let record = base64_codec.encode(&data);
        Ok(record)
    }

    fn try_deserialize_entry(record: &str) -> Result<LogEntry, io::Error> {
        let bytes = base64_codec.decode(record).unwrap();  /* De-unwrap:ify. */
        let entry = bincode::deserialize(&bytes).unwrap(); /* De-unwrap:ify. */
        Ok(entry)
    }

    fn sync(&self) -> Result<(), io::Error> {
        self.file.sync_all()
    }

    pub fn replay(&self) -> Result<ReplayView, io::Error> {
        Ok(ReplayView::new(fs::File::open(&self.path)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::temp_dir;
    use arbitrary::*;
    use rand::Rng;

    fn truncate(path: &path::Path) -> Result<(), io::Error> {
        fs::OpenOptions::new().write(true).create(true).truncate(true).open(path)?;
        Ok(())
    }

    fn log_entry(m: resp::Message) -> LogEntry {
        LogEntry { 
            at: time::SystemTime::now(), 
            revision: Revision::default(), 
            content: m.into(),
        }
    }

    #[test]
    fn end_to_end() {
        let path = temp_dir().with_file_name("transactions.log");
        truncate(&path).unwrap();

        let mut log = LogFile::new(&path).unwrap();
        log.append(log_entry(resp::Message::BulkString("Hi, mom".to_string()))).unwrap();
        log.append(log_entry(resp::Message::Integer(427))).unwrap();

        let log = LogFile::new(&path).unwrap();
        assert_eq!(
            log.replay().unwrap().iter().collect::<Result<Vec<resp::Message>, io::Error>>().unwrap(), 
            vec![
                resp::Message::BulkString("Hi, mom".to_string()),
                resp::Message::Integer(427)
            ]
        )
    }

    #[test]
    fn arbitariness() {
        /* Does this even work? */
        let random_bytes = rand::thread_rng().gen::<[u8; 32]>();
        let mut u = Unstructured::new(&random_bytes);
        let ms = u.arbitrary::<Vec<resp::Message>>().unwrap();

        let path = temp_dir().with_file_name("transactions2.log");
        truncate(&path).unwrap();

        let mut log = LogFile::new(&path).unwrap();
        for m in ms.iter() {
            log.append(log_entry(m.clone())).unwrap();
        }
        log.sync().unwrap();

        let log = LogFile::new(&path).unwrap();
        assert_eq!(
            log.replay().unwrap().iter().collect::<Result<Vec<resp::Message>, io::Error>>().unwrap(),
            ms
        );
    }
}