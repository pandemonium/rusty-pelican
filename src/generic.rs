use std::io;
use crate::commands;
use crate::core;
use crate::resp;
use crate::resp::Message;

pub trait Generic {
    fn keys(&self, pattern: &str) -> Vec<String>;
    fn scan(&self, cursor: usize, pattern: Option<&str>, count: Option<usize>, tpe: Option<&str>);
}

impl Generic for core::PersistentState {
    fn keys(&self, _pattern: &str) -> Vec<String> {
        self.strings.keys()
            .chain(self.lists.keys())
            .filter_map(|s| /* Eval glob pattern. */ Some(s.to_string()))
            .collect()
    }

    fn scan(
        &self, 
        _cursor: usize, 
        _pattern: Option<&str>, 
        _count: Option<usize>, 
        _tpe: Option<&str>
    ) {
        todo!()
    }
}

pub fn apply(
    state: &core::State,
    command: commands::Generic,
)  -> Result<resp::Message, io::Error> {
    match command {
        commands::Generic::Keys(pattern) => 
            Ok(Message::make_bulk_array(
                state.for_reading()?.keys(&pattern).as_slice()
            )),
        commands::Generic::Scan { cursor: _, pattern: _, count: _, tpe: _ } => 
            todo!(),
    }
}