use std::collections;
use std::thread;
use std::sync;
use std::io;
use std::io::prelude::*;
use std::net;
use std::ops::Deref;

use crate::commands::*;
use crate::resp::*;
use crate::datatype::*;
use crate::generic;
use crate::connections;
use crate::server;
use crate::ttl;
use crate::persistence;
use parser::*;

pub type Domain = persistence::WithTransactionLog<ttl::Lifetimes<Dataset>>;

#[derive(Clone)]
pub struct DomainContext(sync::Arc<sync::RwLock<Domain>>);

impl DomainContext {
    pub fn new(domain: Domain) -> Result<Self, io::Error> {
        Ok(Self(sync::Arc::new(sync::RwLock::new(domain))))
    }

    pub fn for_reading(&self) -> Result<sync::RwLockReadGuard<Domain>, io::Error> {
        self.0.read().map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    pub fn for_writing(&self) -> Result<sync::RwLockWriteGuard<Domain>, io::Error> {
        self.0.write().map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    fn replay_transactions(&self) -> Result<persistence::ReplayView, io::Error> {
        self.for_reading()?.transaction_log().replay()
    }

    pub fn apply_transaction_log(&self) -> Result<(), io::Error> {
        for message in self.replay_transactions()?.iter() {
            Command::try_from(&message).and_then(|command|
                self.apply(CommandContext::new(command, message))
            )?;
        }

        self.for_writing()?.finalize_replay();

        Ok(())
    }
}

impl ttl::Expungeable for Dataset {
    type Key = String;
    fn expunge(&mut self, id: &Self::Key) {
        self.lists.remove(id);
        self.strings.remove(id);
    }
}

pub struct Dataset {
    pub lists:   collections::HashMap<String, collections::VecDeque<String>>,
    pub strings: collections::HashMap<String, String>,
}

impl Dataset {
    pub fn empty() -> Self {
        Self { 
            lists:   collections::HashMap::new(),
            strings: collections::HashMap::new(),
        }
    }
}

trait Executive {
    fn apply(&self, command: CommandContext<Command>) -> Result<Message, io::Error>;
}

#[derive(Clone)]
pub struct CommandContext<A: Clone> {
    command: A,
    message: Message,
}

impl <A: Clone> Deref for CommandContext<A> {
    type Target = A;

    fn deref(&self) -> &Self::Target {
        &self.command
    }
}

impl <A: Clone> CommandContext<A> {
    fn new(command: A, message: Message) -> Self {
        Self { command, message }
    }

    pub fn request_message(&self) -> Message {
        self.message.clone()
    }
}

impl Executive for DomainContext {
    fn apply(&self, command: CommandContext<Command>) -> Result<Message, io::Error> {
        match &*command {
            Command::Lists(sub_command) =>
                lists::apply(self, CommandContext::new(sub_command.clone(), command.request_message())),
            Command::Strings(sub_command) => 
                keyvalue::apply(self, CommandContext::new(sub_command.clone(), command.request_message())),
            Command::Generic(sub_command) => 
                generic::apply(self, CommandContext::new(sub_command.clone(), command.request_message())),
            Command::ConnectionManagement(sub_command) => 
                connections::apply(self, sub_command.clone()),
            Command::ServerManagement(sub_command) => 
                server::apply(self, sub_command.clone()),
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
    pub fn make(domain: DomainContext, interface: &str) -> Result<Self, io::Error> {
        let listener = net::TcpListener::bind(interface)?;
        Ok(Self {
            state: domain,
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
        request.parse_message(reader).and_then(|message| {
            Command::try_from(&message).and_then(|command|
                state.apply(CommandContext::new(command, message))
            )
        })
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