use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::cmp;
use std::thread;

use crate::commands::*;
use crate::resp::*;

pub struct PersistentState {
    lists:   HashMap<String, Vec<String>>,
    strings: HashMap<String, String>,
}

impl PersistentState {
    pub fn make() -> Self {
        Self {
            lists: HashMap::new(),
            strings: HashMap::new(),
        }
    }

    pub fn restore_from_disk(&mut self, home: &Path) -> Result<(), Error> {
        println!("Loading transaction log.");
        Ok(())
    }
}

mod schema {
    pub trait Schema {
        fn keys(&self, pattern: &str) -> Vec<String>;
    }
}

/* mkdir datatype, copy all list stuff into lists.rs, etc. */
mod datatype {
    pub trait List {
        fn range(&self, key: &str, start: i32, stop: i32) -> Vec<String>;
        fn append(&mut self, key: &str, element: &str) -> usize;
        fn prepend(&mut self, key: &str, element: &str) -> usize;
        fn length(&self, key: &str) -> usize;
    }
}

impl schema::Schema for PersistentState {
    fn keys(&self, pattern: &str) -> Vec<String> {
        self.strings.keys()
            .chain(self.lists.keys())
            .filter_map(|s| /* Eval glob pattern. */ Some(s.to_string()))
            .collect()
    }
}

impl datatype::List for PersistentState {
    fn range(&self, key: &str, start: i32, stop: i32) -> Vec<String> {
        let length = self.length(key) as i32;
        if start >= length {
            vec![]
        } else {
            /* Indices don't wrap around. So an effective stop left of start ranges
               over an empty list. */
            let effective_start = ((start + length) % length) as usize;

            // This isn't quite correct, is it?
            let effective_stop = ((stop + length) % length) as usize;

            if effective_start <= effective_stop {
                // range end index 18446744073709551615 out of range for slice of length 7',
                self.lists[key][effective_start..effective_stop].to_vec()
            } else {
                self.lists[key][effective_stop..length as usize].to_vec()
            }
        }
    }

    fn append(&mut self, key: &str, element: &str) -> usize {
        self.lists
            .entry(key.to_string())
            .and_modify(|xs| xs.push(element.to_string()))
            .or_insert(vec![element.to_string()]);
        self.length(key)
    }

    fn prepend(&mut self, key: &str, element: &str) -> usize {
        self.lists
            .entry(key.to_string())
            .and_modify(|xs| xs.insert(0, element.to_string()))
            .or_insert(vec![element.to_string()]);
        self.length(key)
    }

    fn length(&self, key: &str) -> usize {
        self.lists
            .get(key).map_or(0, |v| v.len())
    }
}

trait Executive {
    /* Commands are generally not applicable to any data-type. */
    fn apply(&self, command: Command) -> Result<Message, Error>;
}

use std::sync;

/* The default Command Processor. */
impl Executive for sync::Arc<sync::RwLock<PersistentState>> {
    fn apply(&self, command: Command) -> Result<Message, Error> {
        use schema::Schema;
        use datatype::List;
        use crate::commands::List as ListCommand;
        use crate::commands::Schema as SchemaCommand;

        /* This needs to be split into several sub-groups. */
        match command {
            /* I want a process_list function, but don't seem to be permitted one. */
            Command::List(ListCommand::Range(key, start, stop)) => {
                let elements =
                    self.read()
                        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?
                        .range(&key, start, stop)
                        .into_iter()
                        .map(Message::BulkString)
                        .collect();
                Ok(Message::Array(elements))
            },
            Command::List(ListCommand::Length(key)) => {
                let return_value = 
                    self.read()
                        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?
                        .length(&key);
                Ok(Message::Integer(return_value as i64))
            },
            Command::List(ListCommand::Append(key, elements)) => {
                let mut return_value = 0;
                for element in elements {
                    return_value = 
                        self.write()
                            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?
                            .append(&key, &element);
                };
                Ok(Message::Integer(return_value as i64))
            },
            Command::List(ListCommand::Prepend(key, elements)) => {
                let mut return_value = 0;
                for element in elements {
                    return_value = 
                        self.write()
                            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?
                            .prepend(&key, &element);
                };
                Ok(Message::Integer(return_value as i64))
            },
            Command::Client(Client::SetName(name)) => {
                println!("Executive::apply: Set client name {name}");
                Ok(Message::SimpleString("OK".to_string()))
            },
            Command::Info(Info::Server) => {
                println!("Executive::apply: Info about server");
                let server_info = "# Server\r\nredis_version:7.0.9\r\n";
                Ok(Message::BulkString(server_info.to_string()))
            },
            Command::Info(Info::Keyspace) => {
                println!("Executive::apply: Info about keyspace");
                let keys =
                    self.read()
                        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?
                        .keys("*");
                let keyspace = format!("# Keyspace\r\ndb0:keys={},expires=0,avg_ttl=0\r\n", keys.len());
                Ok(Message::BulkString(keyspace))
            },
            Command::Info(Info::Category(name)) => {
                println!("Executive::apply: Info about {name}");
                Ok(Message::Error { prefix: ErrorPrefix::Err, message: "Unsupported command".to_string() })
            },
            Command::Other(Miscellaneous::Schema(SchemaCommand::Select(_))) => {
                Ok(Message::SimpleString("OK".to_string()))
            },
            Command::Other(Miscellaneous::Schema(SchemaCommand::Keys(pattern))) => {
                let keys =
                    self.read()
                        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?
                        .keys(&pattern).into_iter()
                        .map(Message::BulkString)
                        .collect();
                Ok(Message::Array(keys))
            },
            Command::Other(Miscellaneous::DbSize) => {
                let keys =
                    self.read()
                        .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?
                        .keys("*")
                        .len();
                Ok(Message::Integer(keys as i64))
            },
            Command::Other(Miscellaneous::Ping(message)) => {
                Ok(Message::SimpleString(message))
            },
            Command::Other(Miscellaneous::Docs) => {
                Ok(Message::Error { prefix: ErrorPrefix::Err, message: "Unsupported command".to_string() })
            },
            Command::Other(Miscellaneous::Empty) => {
                Ok(Message::Error { prefix: ErrorPrefix::Err, message: "Unsupported command".to_string() })
            },
            Command::Other(Miscellaneous::Unknown(command)) => {
                println!("Unknown command: {command}");
                Ok(Message::Error { prefix: ErrorPrefix::Err, message: "Unsupported command".to_string() })
            },
        }
    }
}

pub mod server {
    use super::*;
    use std::{
        io::{prelude::*, BufReader, BufWriter},
        net::{TcpListener, TcpStream},
    };
    use parser::*;
    use std::sync;

    pub struct RunLoop {
        state: sync::Arc<sync::RwLock<PersistentState>>,
        server_socket: TcpListener,
    }

    impl RunLoop {
        pub fn make(state: PersistentState, iface:  &str) -> Result<Self, Error> {
            let listener = TcpListener::bind(iface)?;
            Ok(Self {
                state: sync::Arc::new(sync::RwLock::new(state)),
                server_socket: listener,
            })
        }

        pub fn execute(&self) -> Result<(), Error> {
            let server = self.server_socket.try_clone()?; /* Haha. */
            for connection in server.incoming() {
                let state = self.state.clone();
                match connection {
                    Ok(socket) => {
                        thread::spawn(move || Self::handle_connection(state, &socket));
                        ()
                    },
                    Err(e) => println!("execute: Error `{}`.", e),
                }
            }
            Ok(())
        }

        fn handle_connection(
            state: sync::Arc<sync::RwLock<PersistentState>>,
            connection: &TcpStream
        ) {
            let mut reader = BufReader::new(connection);
            let mut writer = BufWriter::new(connection);
            let mut request = RequestState::make();

            /* Clean up this mess. Replace with ?-syntax. */
            loop {
                match request.read(&mut reader) {
                    Ok(message) =>
                        match Self::handle_request(&state, message, &mut writer) {
                            Ok(_) => (),
                            Err(e) => println!("handle_connection: Error `{}`.", e),
                        },
                    Err(_) => {
                        let message = request.as_unknown_command_error_message();
                        match writer.write_all(String::from(message).as_bytes()) {
                            Ok(_) => (),
                            Err(e) => println!("handle_connection: Error responding with error `{}`.", e),
                        }
                        break;
                    },
                }                
            }

        }

        fn handle_request<A: Write>(
            state: &sync::Arc<sync::RwLock<PersistentState>>,
            request: Message, 
            out: &mut BufWriter<A>
        ) -> Result<(), Error> {
            let command = Command::try_from(request)?;
            let response = state.apply(command)?;
            println!("handle_request: responding with `{}`.", response);
            out.write_all(String::from(response).as_bytes())?;
            out.flush()
        }
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