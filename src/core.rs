use std::collections::HashMap;
use std::io::Error;
use std::path::Path;

use crate::commands::*;
use crate::resp::*;

struct State {
    lists:   HashMap<String, Vec<String>>,
    strings: HashMap<String, String>,
}

impl State {
    fn make() -> Self {
        Self {
            lists: HashMap::new(),
            strings: HashMap::new(),
        }
    }

    fn restore_from_disk(home: &Path) -> Option<Self> {
        todo!()
    }
}

mod datatype {
    pub trait List {
        fn push(&mut self, key: &str, element: &str) -> usize;
        fn length(&self, key: &str) -> usize;
    }
}

impl datatype::List for State {
    fn push(&mut self, key: &str, element: &str) -> usize {
        self.lists
            .entry(key.to_string())
            .and_modify(|xs| xs.push(element.to_string()))
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
    fn apply(&mut self, command: Command) -> Result<Value, Error>;
}

/* The default Command Processor. */
impl Executive for State {
    fn apply(&mut self, command: Command) -> Result<Value, Error> {
        use datatype::List;
        use crate::commands::List as ListCommand;

        match command {
            /* I want a process_list function, but don't seem to be permitted one. */
            Command::List(ListCommand::Length(key)) => {
                let return_value = self.length(key.as_str());
                Ok(Value::Integer(return_value as i64))
            },
            Command::List(ListCommand::Push(key, elements)) => {
                let mut return_value = 0;
                for element in elements {
                    return_value = self.push(key.as_str(), element.as_str());
                };
                Ok(Value::Integer(return_value as i64))
            },
        }
    }
}

mod server {
    use super::*;
    use std::{
        io::{prelude::*, BufReader},
        net::{TcpListener, TcpStream},
    };

    struct RunLoop {
        state: State,
        server_socket: TcpListener,
    }

    impl RunLoop {
        fn make(state: State, iface:  &str) -> Result<Self, Error> {
            let listener = TcpListener::bind(iface)?;
            Ok(Self {
                state: state,
                server_socket: listener,
            })
        }

        fn execute(&self) -> Result<(), Error> {
            loop {
                let incoming = self.server_socket.incoming();
                for client in incoming {
                    self.run_client(client?);
                }
            }
        }

        fn run_client(&self, client: TcpStream) {
            let mut reader = BufReader::new(client);
            let mut buffer = String::new();

            /* Will this work? When has it read enough for one request? */
            match reader.read_to_string(&mut buffer) {
                Ok(s) => todo!(),
                Err(e) => todo!(),
            }
        }
    }
}