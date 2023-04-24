pub mod snapshots;
pub mod tx_log;
pub mod domain;
pub mod resp;

use std::collections;
use std::thread;
use std::sync;
use std::io;
use std::io::prelude::*;
use std::net;
use std::ops::Deref;
use serde::{Serialize, Deserialize};

use crate::commands::*;
use domain::*;
use ttl::Lifetimes;
use crate::generic;
use crate::connections;
use crate::server;
use crate::core::domain::ttl;
use tx_log::WriteTransactionSink;
use snapshots::Snapshots;
use resp::*;
use resp::parser::*;

pub type State = tx_log::LoggedTransactions<ttl::Lifetimes<Datasets>>;

#[derive(Clone)]
pub struct StateContext(sync::Arc<sync::RwLock<State>>);

impl StateContext {
    pub fn new(state: State) -> Self {
        /* Is Arc really needed here? It's not really passed around.
           RwLock is not clonable. Replace Arc with Box perhaps. */
        Self(sync::Arc::new(sync::RwLock::new(state)))
    }

    pub fn begin_reading(&self) -> io::Result<sync::RwLockReadGuard<State>> {
        self.0.read().map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    pub fn begin_writing(&self) -> io::Result<sync::RwLockWriteGuard<State>> {
        self.0.write().map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))
    }

    pub fn apply_transaction<F, A, C>(
        &self, 
        command: &CommandContext<C>,
        unit_of_work: F
    ) -> io::Result<A>
    where 
        F: FnOnce(&mut State) -> A,
        C: Clone,
    {
        let mut state = self.begin_writing()?;
        let return_value = unit_of_work(&mut state);
        let revision = &state.revision();
        state.record_evidence(revision, command.transaction_message())?;
        state.bump_revision();
        Ok(return_value)
    }

    pub fn restore_from_disk(&mut self) -> io::Result<()> {
        self.restore_most_recent_snapshot()?;
        self.apply_transaction_log()
    }

    fn restore_most_recent_snapshot(&mut self) -> io::Result<()> {
        self.begin_writing()?.restore_most_recent_snapshot()
    }

    fn apply_transaction_log(&self) -> io::Result<()> {
        {   let state = self.begin_reading()?;
            for message in state.transaction_log().replay(&state.revision())?.iter() {
                self.apply(CommandContext::try_from(&message?)?)?;
            }
        }

        Ok(self.begin_writing()?.finalize_replay())
    }
}

impl ttl::Expungeable for Datasets {
    fn expunge(&mut self, id: &str) {
        /* Should this take a transaction logged route instead? */
        if self.lists.remove(id).is_none() {
            if self.strings.remove(id).is_none() {
                self.sorted_sets.remove(id);
            }
        }
    }
}

impl snapshots::Snapshots for Lifetimes<Datasets> {
    fn save_snapshot(&self) -> io::Result<()> {
        snapshots::allocate_new()?.put(self)
    }

    fn restore_most_recent_snapshot(&mut self) -> io::Result<()> {
        if let Some(snapshot) = snapshots::most_recent()? {
            *self = snapshot.get::<Self>()?;
        }
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
pub struct SortedSetEntry {
    score: f64,
    member: String,
}

type Keyed<A> = collections::HashMap<String, A>;

fn new_keyed<A>() -> Keyed<A> { collections::HashMap::new() }

#[derive(Deserialize, Serialize)]
pub struct Datasets {
    pub lists:       Keyed<collections::VecDeque<String>>,
    pub strings:     Keyed<String>,
    pub sorted_sets: Keyed<domain::sorted_sets::OrderedScores>,
    revision:        tx_log::Revision,
}

impl Default for Datasets {
    fn default() -> Self { Self::new() }
}

impl Datasets {
    pub fn new() -> Self {
        Self { lists:       new_keyed(),
               strings:     new_keyed(),
               sorted_sets: new_keyed(),
               revision:    tx_log::Revision::default() }
    }

    pub fn revision(&self) -> tx_log::Revision { self.revision.clone() }

    pub fn bump_revision(&mut self) {
        self.revision = self.revision().succeeding();
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.lists.keys().chain(
            self.strings.keys().chain(
                self.sorted_sets.keys()
            )
        )
    }
}

trait Executive {
    fn apply(&self, command: CommandContext<Command>) -> io::Result<Message>;
}

#[derive(Clone)]
pub struct CommandContext<'a, A: Clone> {
    command: A,
    message: &'a Message,
}

impl <'a, A: Clone> Deref for CommandContext<'a, A> {
    type Target = A;
    fn deref(&self) -> &Self::Target { &self.command }
}

impl <'a, A: Clone> CommandContext<'a, A> {
    fn new(command: A, message: &'a Message) -> Self {
        Self { command, message }
    }

    pub fn transaction_message(&self) -> &Message { self.message }
}

impl <'a> TryFrom<&'a Message> for CommandContext<'a, Command> {
    type Error = io::Error;

    fn try_from(message: &'a Message) -> Result<Self, Self::Error> {
        Ok(Self::new(Command::try_from(message)?, message))
    }
}

impl Executive for StateContext {
    fn apply(&self, command: CommandContext<Command>) -> io::Result<Message> {
        match &*command {
            Command::Lists(sub_command) =>
                lists::apply(self, CommandContext::new(sub_command.clone(), command.transaction_message())),
            Command::Strings(ref sub_command) =>
                keyvalues::apply(self, CommandContext::new(sub_command.clone(), command.transaction_message())),
            Command::SortedSets(ref sub_command) =>
                sorted_sets::apply(self, CommandContext::new(sub_command.clone(), command.transaction_message())),
            Command::Generic(ref sub_command) =>
                generic::apply(self, CommandContext::new(sub_command.clone(), command.transaction_message())),
            Command::ConnectionManagement(ref sub_command) =>
                connections::apply(self, &sub_command),
            Command::ServerManagement(ref sub_command) =>
                server::apply(self, &sub_command),
            Command::Unknown(ref name) =>
                Ok(Message::Error {
                    prefix: ErrorPrefix::Err,
                    message: format!("Unsupported command string `{name}`."),
                }),
        }
    }
}

pub struct RunLoop {
    state:    StateContext,
    listener: net::TcpListener,
}

impl RunLoop {
    pub fn new(state: StateContext, interface: &str) -> io::Result<Self> {
        Ok(Self { state, listener: net::TcpListener::bind(interface)? })
    }

    pub fn execute(&self) -> io::Result<()> {
        let listener = self.listener.try_clone()?;
        for connection in listener.incoming() {
            match connection {
                Ok(socket) => {
                    let state = self.state.clone();
                    thread::spawn(move || Self::handle_connection(state, socket));
                },
                Err(e) => println!("execute: Error `{e}`."),
            }
        }
        Ok(())
    }

    fn handle_connection(state: StateContext, connection: net::TcpStream) -> io::Result<()> {
        let mut reader = io::BufReader::new(&connection);
        let mut writer = io::BufWriter::new(&connection);
        loop {
            let message = read_message(&mut reader)?;
            let command = CommandContext::try_from(&message)?;
            let response = state.apply(command)?;

            println!("handle_request: responding with `{response}`.");
            writer.write_all(String::from(response).as_bytes())?;
            writer.flush()?;
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