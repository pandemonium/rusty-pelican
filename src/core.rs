use std::collections::HashMap;
use std::path::Path;
use std::thread;
use std::sync;
use std::io;

use crate::commands::*;
use crate::resp::*;
use crate::datatype::*;
use crate::generic;
use crate::connections;
use crate::server;

use std::{
    io::{prelude::*, BufReader, BufWriter},
    net::{TcpListener, TcpStream},
};
use parser::*;

#[derive(Clone)]
pub struct State(sync::Arc<sync::RwLock<PersistentState>>);

impl State {
    pub fn for_reading(&self) -> Result<sync::RwLockReadGuard<PersistentState>, io::Error> {
        self.0.read().map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    pub fn for_writing(&self) -> Result<sync::RwLockWriteGuard<PersistentState>, io::Error> {
        self.0.write().map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }
}

pub struct PersistentState {
    pub lists:   HashMap<String, Vec<String>>,
    pub strings: HashMap<String, String>,
}

impl PersistentState {
    pub fn make() -> Self {
        Self { lists: HashMap::new(),
               strings: HashMap::new(), }
    }

    pub fn restore_from_disk(&mut self, _home: &Path) -> Result<(), io::Error> {
        println!("Loading transaction log.");
        Ok(())
    }
}

trait Executive {
    fn apply(&self, command: Command) -> Result<Message, io::Error>;
}

impl Executive for State {
    fn apply(&self, command: Command) -> Result<Message, io::Error> {
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
    state: State,
    server_socket: TcpListener,
}

impl RunLoop {
    pub fn make(state: PersistentState, iface:  &str) -> Result<Self, io::Error> {
        let listener = TcpListener::bind(iface)?;
        Ok(Self {
            state: State(sync::Arc::new(sync::RwLock::new(state))),
            server_socket: listener,
        })
    }

    pub fn execute(&self) -> Result<(), io::Error> {
        let server = self.server_socket.try_clone()?;
        for connection in server.incoming() {
            let state = self.state.clone();
            match connection {
                Ok(socket) => {
                    thread::spawn(move || Self::handle_connection(&state, &socket));
                    ()
                },
                Err(e) => println!("execute: Error `{}`.", e),
            }
        }
        Ok(())
    }

    fn handle_connection(state: &State, connection: &TcpStream) -> Result<(), io::Error> {
        let mut reader = BufReader::new(connection);
        let mut writer = BufWriter::new(connection);
        loop {
            let response = Self::handle_request(state, &mut reader)?;

            println!("handle_request: responding with `{}`.", response);
            writer.write_all(String::from(response).as_bytes())?;
            writer.flush()?  
        }
    }

    fn handle_request(state: &State, reader: &mut BufReader<&TcpStream>) -> Result<Message, io::Error> {
        let mut request = RequestState::make();
        request.read(reader).and_then(|message|
            Command::try_from(message).and_then(|command| state.apply(command))
        ).or_else(|_error| {
            Ok::<Message, io::Error>(request.as_unknown_command_error_message())
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