use std::collections;
use std::path::Path;
use std::thread;
use std::sync;
use std::io;
use std::io::prelude::*;
use std::net;

use crate::commands::*;
use crate::resp::*;
use crate::datatype::*;
use crate::generic;
use crate::connections;
use crate::server;
use crate::ttl;
use crate::persistence;

use parser::*;

pub type Domain = persistence::WithTransactionLog<ttl::Lifetimes<Data>>;

#[derive(Clone)]
pub struct DomainContext(sync::Arc<sync::RwLock<Domain>>);

impl DomainContext {
    pub fn new(data: Data) -> Result<Self, io::Error> {
        Ok(Self(sync::Arc::new(sync::RwLock::new(
            persistence::WithTransactionLog::new(
                ttl::Lifetimes::new(data)
            )?
        ))))
    }

    pub fn for_reading(&self) -> Result<sync::RwLockReadGuard<Domain>, io::Error> {
        self.0.read().map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    pub fn for_writing(&self) -> Result<sync::RwLockWriteGuard<Domain>, io::Error> {
        self.0.write().map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }
}

impl ttl::Expungeable for Data {
    type Key = String;
    fn expunge(&mut self, id: &Self::Key) {
        self.lists.remove(id);
        self.strings.remove(id);
    }
}

pub struct Data {
    pub lists:   collections::HashMap<String, collections::VecDeque<String>>,
    pub strings: collections::HashMap<String, String>,
}

impl Data {
    pub fn empty() -> Self {
        Self { 
            lists:   collections::HashMap::new(),
            strings: collections::HashMap::new(),
        }
    }

    pub fn restore_from_disk(&mut self, _home: &Path) -> Result<(), io::Error> {
        println!("Loading transaction log.");
        Ok(())
    }
}

trait Executive {
    fn apply(&self, command: Command) -> Result<Message, io::Error>;
}

struct CommandContext<C> {
    command: C,
    message: Message,
}

impl Executive for DomainContext {
    fn apply(&self, command: Command) -> Result<Message, io::Error> {
//        self.for_writing()?.record_write(todo!())?;
        match command {
            Command::Lists(command)                => lists::apply(self, command),
            Command::Strings(command)              => keyvalue::apply(self, command),
            Command::Generic(command)              => generic::apply(self, command),
            Command::ConnectionManagement(command) => connections::apply(self, command),
            Command::ServerManagement(command)     => server::apply(self, command),
            Command::Unknown(name) =>
                Ok(Message::Error { 
                    prefix: ErrorPrefix::Err,
                    message: format!("Unsupported command string `{name}`."), 
                }),
        }
    }
}

pub struct RunLoop {
    state:         DomainContext,
    socket_server: net::TcpListener,
}

impl RunLoop {
    pub fn make(data: Data, interface: &str) -> Result<Self, io::Error> {
        let listener = net::TcpListener::bind(interface)?;
        Ok(Self {
            state: DomainContext::new(data)?,
            socket_server: listener,
        })
    }

    pub fn execute(&self) -> Result<(), io::Error> {
        let server = self.socket_server.try_clone()?;
        for connection in server.incoming() {
            let state = self.state.clone();
            match connection {
                Ok(socket) => {
                    thread::spawn(move || Self::handle_connection(&state, &socket));
                },
                Err(e) => println!("execute: Error `{}`.", e),
            }
        }
        Ok(())
    }

    fn handle_connection(state: &DomainContext, connection: &net::TcpStream) -> Result<(), io::Error> {
        let mut reader = io::BufReader::new(connection);
        let mut writer = io::BufWriter::new(connection);
        loop {
            let response = Self::handle_command(state, &mut reader)?;

            println!("handle_request: responding with `{}`.", response);
            writer.write_all(String::from(response).as_bytes())?;
            writer.flush()?
        }
    }

    fn handle_command(state: &DomainContext, reader: &mut io::BufReader<&net::TcpStream>) -> Result<Message, io::Error> {
        let mut request = ParseState::empty();
        request.parse_message(reader).and_then(|message|
            /* Commands that write, need their Message added
               to the TransactionLog. */
            Command::try_from(message).and_then(|command| state.apply(command))
        )
    }
}


#[cfg(test)]
mod tests {
    /* How is this tested? */

//    #[test]
//    fn questionable_syntax() {
//        let xs = vec![Ok(10), Err("hi, mom")];
//        for x in xs {
//            assert_eq!(x?, 10)
//        }
//    }
}