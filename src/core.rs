use std::collections::HashMap;
use std::io::Error;
use std::path::Path;

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

impl datatype::List for PersistentState {
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
    fn apply(&mut self, command: Command) -> Result<Message, Error>;
}

/* The default Command Processor. */
impl Executive for PersistentState {
    fn apply(&mut self, command: Command) -> Result<Message, Error> {
        use datatype::List;
        use crate::commands::List as ListCommand;

        match command {
            /* I want a process_list function, but don't seem to be permitted one. */
            Command::List(ListCommand::Length(key)) => {
                let return_value = self.length(key.as_str());
                Ok(Message::Integer(return_value as i64))
            },
            Command::List(ListCommand::Push(key, elements)) => {
                let mut return_value = 0;
                for element in elements {
                    return_value = self.push(key.as_str(), element.as_str());
                };
                Ok(Message::Integer(return_value as i64))
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

    pub struct RunLoop {
        state: PersistentState,
        server_socket: TcpListener,
    }

    impl RunLoop {
        pub fn make(state: PersistentState, iface:  &str) -> Result<Self, Error> {
            let listener = TcpListener::bind(iface)?;
            Ok(Self {
                state: state,
                server_socket: listener,
            })
        }

        pub fn execute(&mut self) -> Result<(), Error> {
            let server = self.server_socket.try_clone()?; /* Haha. */
            for connection in server.incoming() {
                match connection {
                    Ok(socket) => self.handle_connection(&socket),
                    Err(e)     => println!("execute: Error `{}`.", e),
                }
            }
            Ok(())
        }

        fn handle_connection(&mut self, connection: &TcpStream) {
            let mut reader = BufReader::new(connection);
            let mut request = RequestState::make();
            request.read(&mut reader);
            let mut writer = BufWriter::new(connection);
            match request.try_parse_message() {
                Some(message) =>
                    match self.handle_request(message, &mut writer) {
                        Ok(_) => println!("handle_connection: Great success."),
                        Err(e) => println!("handle_connection: Error `{}`.", e),
                    },
                None => /* Construct an error value. */ todo!(),
            }
        }

        fn handle_request<A: Write>(
            &mut self,
            request: Message, 
            out: &mut BufWriter<A>
        ) -> Result<(), Error> {
            let command = Command::try_from(request)?;
            let response = self.state.apply(command)?;
            out.write_all(String::from(response).as_bytes())
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