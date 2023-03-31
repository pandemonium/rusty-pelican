use std::fs;
use std::io;
use std::io::{BufRead, Write};
use std::path;
use std::ops::{Deref, DerefMut};
use std::time;
use std::str;
use serde::{Deserialize, Serialize};
use base64::{
    Engine as _, 
    engine::general_purpose::STANDARD_NO_PAD as base64_codec
};

use crate::resp;

#[derive(Clone, Default, Serialize, Deserialize, PartialEq, PartialOrd)]
pub struct Revision(usize);

impl Revision {
    pub fn succeeding(&self) -> Self {
        Self(self.0 + 1)
    }
}

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
            log: LogFile::new(default_path)?,
            underlying,
            replaying: true,
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

pub struct ReplayView {
    file: fs::File,
    since: Revision,
}

impl ReplayView {
    fn new(file: fs::File, since: Revision) -> Self {
        Self { file, since }
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<resp::Message, io::Error>> + '_ {
        let reader = io::BufReader::new(&self.file);
        reader.lines()
              .map(|record| LogEntry::try_from(record?))
              .skip_while(|entry| entry.as_ref().map_or(false, |e| e.revision < self.since))
              .map(|record| record?.content.parse())
    }
}

impl TryFrom<String> for LogEntry {
    type Error = io::Error;

    /* Error handling is really bad at this point. */

    fn try_from(record: String) -> Result<Self, Self::Error> {
        let bytes = base64_codec.decode(record).map_err(|e|
            io::Error::new(io::ErrorKind::Other, e.to_string())
        )?;
        bincode::deserialize(&bytes).map_err(|e|
            io::Error::new(io::ErrorKind::Other, e.to_string())
        )
    }
}

impl TryFrom<LogEntry> for String {
    type Error = io::Error;

    /* Error handling is really bad at this point. */

    fn try_from(entry: LogEntry) -> Result<Self, Self::Error> {
        let data = bincode::serialize(&entry).map_err(|e|
            io::Error::new(io::ErrorKind::Other, e.to_string())
        )?;
        Ok(base64_codec.encode(data))
    }
}

pub struct LogFile {
    path: path::PathBuf,
    file: fs::File,
}

impl LogFile {
    fn new(at: &path::Path) -> Result<Self, io::Error> {
        Ok(Self {
            path: at.into(),
            file: fs::File::options().append(true).create(true).open(at)?,
        })
    }

    fn append(&mut self, entry: LogEntry) -> Result<(), io::Error> {
        let record: String = entry.try_into()?;
        self.file.write_all(format!("{record}\r\n").as_bytes())
        /* if now > fs_sync deadline { file.fs_sync() } */
    }

    fn sync(&self) -> Result<(), io::Error> {
        self.file.sync_all()
    }

    pub fn replay(&self, since: &Revision) -> Result<ReplayView, io::Error> {
        Ok(ReplayView::new(fs::File::open(&self.path)?, since.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env::temp_dir;
    use arbitrary::*;
    use rand::{distributions::Alphanumeric, Rng};

    fn log_entry(m: resp::Message) -> LogEntry {
        LogEntry { 
            at: time::SystemTime::now(), 
            revision: Revision::default(), 
            content: m.into(),
        }
    }

    fn generate_name() -> String {
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(25)
            .map(char::from)
            .collect()
    }

    fn temp_file() -> path::PathBuf {
        let file_name = generate_name();
        temp_dir().with_file_name(file_name)
    }

    #[test]
    fn discards_stale_prefix() {
        fn mk_entry(rev: &Revision, msg: resp::Message) -> LogEntry {
            LogEntry::new(time::SystemTime::now(), rev, &msg)
        }

        fn mk_string(text: &str) -> resp::Message {
            resp::Message::SimpleString(text.to_string())
        }

        let path = temp_file();
        let mut log = LogFile::new(&path).unwrap();

        let rev = Revision::default();
        log.append(mk_entry(&rev, mk_string("OK"))).unwrap();
        log.append(mk_entry(&rev.succeeding(), mk_string("OK2"))).unwrap();
        log.append(mk_entry(&rev.succeeding().succeeding(), mk_string("OK3"))).unwrap();

        let log = LogFile::new(&path).unwrap();
        assert_eq!(
            log.replay(&rev.succeeding()).unwrap().iter().collect::<Result<Vec<resp::Message>, io::Error>>().unwrap(),
            vec![mk_string("OK2"), mk_string("OK3")]
        )
    }

    #[test]
    fn end_to_end() {
        let path = temp_file();
        let mut log = LogFile::new(&path).unwrap();

        log.append(log_entry(resp::Message::BulkString("Hi, mom".to_string()))).unwrap();
        log.append(log_entry(resp::Message::Integer(427))).unwrap();

        let log = LogFile::new(&path).unwrap();
        assert_eq!(
            log.replay(&Revision::default()).unwrap().iter().collect::<Result<Vec<resp::Message>, io::Error>>().unwrap(), 
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

        let path = temp_file();
        let mut log = LogFile::new(&path).unwrap();

        for m in ms.iter() {
            log.append(log_entry(m.clone())).unwrap();
        }
        log.sync().unwrap();

        let log = LogFile::new(&path).unwrap();
        assert_eq!(
            log.replay(&Revision::default()).unwrap().iter().collect::<Result<Vec<resp::Message>, io::Error>>().unwrap(),
            ms
        );
    }
}