use std::io;
use std::cmp;

use crate::commands;
use crate::core;
use crate::resp;
use crate::resp::Message;

pub trait List {
    fn range(&self, key: &str, start: i32, stop: i32) -> Vec<String>;
    fn append(&mut self, key: &str, element: &str) -> usize;
    fn prepend(&mut self, key: &str, element: &str) -> usize;
    fn length(&self, key: &str) -> usize;
}

impl List for core::PersistentState {
    fn range(&self, key: &str, start: i32, stop: i32) -> Vec<String> {
        let length = self.length(key) as i32;
        if start >= length {
            vec![]
        } else {
            let effective_start = ((start + length) % length) as usize;
            let effective_stop = if stop < 0 {
                ((stop + length) % length + 1) as usize
            } else {
                cmp::min(stop, length) as usize
            };

            if effective_start <= effective_stop {
                self.lists[key][effective_start..effective_stop].to_vec()
            } else {
                vec![]
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


pub fn apply(
    state:   &core::State, 
    command: commands::ListVerb
) -> Result<resp::Message, io::Error> {
    match command {
        commands::ListVerb::Length(key) =>
            Ok(Message::Integer(
                state.for_writing()?.length(&key) as i64
            )),
        commands::ListVerb::Append(key, elements) => {
            let mut st = state.for_writing()?;
            let mut return_value = 0;
            for element in elements {
                return_value = st.append(&key, &element)
            }
            Ok(Message::Integer(return_value as i64))
        },
        commands::ListVerb::Prepend(key, elements) => {
            let mut st = state.for_writing()?;
            let mut return_value = 0;
            for element in elements {
                return_value = st.prepend(&key, &element)
            }
            Ok(Message::Integer(return_value as i64))
        },
        commands::ListVerb::Range(key, start, stop) =>
            Ok(Message::make_bulk_array(
                state.for_reading()?
                     .range(&key, start, stop)
                     .as_slice()
            )),
    }
}
