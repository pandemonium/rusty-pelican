use std::io;
use std::cmp;
use std::time;

use crate::commands;
use crate::core;
use crate::resp;

pub trait List {
    fn range(&self, key: &str, start: i32, stop: i32) -> Vec<String>;
    fn append(&mut self, key: &str, element: &str) -> usize;
    fn prepend(&mut self, key: &str, element: &str) -> usize;
    fn length(&self, key: &str) -> usize;
}

impl List for core::DomainState {
    fn range(&self, key: &str, start: i32, stop: i32) -> Vec<String> {
        let length = self.length(key) as i32;
        if start >= length {
            vec![]
        } else {
            // Division by zero on length == 0.
            let effective_start = ((start + length) % length) as usize;
            let effective_stop = if stop < 0 {
                ((stop + length) % length + 1) as usize
            } else {
                cmp::min(stop, length) as usize
            };

            if effective_start <= effective_stop {
                self.as_ref().lists[key][effective_start..effective_stop].to_vec()
            } else {
                vec![]
            }
        }
    }

    fn append(&mut self, key: &str, element: &str) -> usize {
        self.as_mut().lists
            .entry(key.to_string())
            .and_modify(|xs| xs.push(element.to_string()))
            .or_insert(vec![element.to_string()]);
        self.expunge_expired(&time::Instant::now());
        self.length(key)
    }

    fn prepend(&mut self, key: &str, element: &str) -> usize {
        self.as_mut().lists
            .entry(key.to_string())
            .and_modify(|xs| xs.insert(0, element.to_string()))
            .or_insert(vec![element.to_string()]);
        self.length(key)
    }

    fn length(&self, key: &str) -> usize {
        self.as_ref().lists
            .get(key).map_or(0, |v| v.len())
    }
}

pub fn apply(
    state:   &core::ServerState, 
    command: commands::ListApi
) -> Result<resp::Message, io::Error> {
    match command {
        commands::ListApi::Length(key) =>
            Ok(resp::Message::Integer(
                state.for_writing()?.length(&key) as i64
            )),
        commands::ListApi::Append(key, elements) => {
            let mut st = state.for_writing()?;
            let mut return_value = 0;
            for element in elements {
                /* Remove deref_mut by AsMut:ing this shit. */
                return_value = st.append(&key, &element)
            }
            Ok(resp::Message::Integer(return_value as i64))
        },
        commands::ListApi::Prepend(key, elements) => {
            let mut st = state.for_writing()?;
            let mut return_value = 0;
            for element in elements {
                return_value = st.prepend(&key, &element)
            }
            Ok(resp::Message::Integer(return_value as i64))
        },
        commands::ListApi::Range(key, start, stop) =>
            Ok(resp::Message::make_bulk_array(
                state.for_reading()?.range(&key, start, stop).as_slice()
            )),
    }
}

#[cfg(test)]
mod tests {
    use crate::core;
    use super::List;

    #[test]
    fn adding() {
        let mut st = core::DomainState::new(core::PersistentState::empty());
        assert_eq!(st.length("key"), 0);
        st.append("key", "1");
        st.append("key", "2");
        st.prepend("key", "3");
        assert_eq!(st.length("key"), 3);
        assert_eq!(st.as_ref().lists.len(), 1);
        st.prepend("key2", "1");
        st.append("key2", "2");
        assert_eq!(st.length("key"), 3);
        assert_eq!(st.length("key2"), 2);
        assert_eq!(st.as_ref().lists.len(), 2);
        assert_eq!(st.as_ref().lists.get("key"), Some(&vec![
            "3".to_string(), "1".to_string(), "2".to_string()
        ]));
        assert_eq!(st.as_ref().lists.get("key2"), Some(&vec![
            "1".to_string(), "2".to_string()
        ]));
    }

    #[test]
    fn range() {
        let mut st = core::DomainState::new(core::PersistentState::empty());
        
        for i in 1..10 {
            st.append("key", &i.to_string());
        }
        assert_eq!(
            st.range("key", 0, 100),
            (1..10).map(|i| i.to_string()).collect::<Vec<_>>()
        );
        assert_eq!(
            st.range("key", 0, -1),
            (1..10).map(|i| i.to_string()).collect::<Vec<_>>()
        );
        assert_eq!(
            st.range("key", 0, -2),
            (1..9).map(|i| i.to_string()).collect::<Vec<_>>()
        );
        assert_eq!(
            st.range("key", 5, -2),
            (6..9).map(|i| i.to_string()).collect::<Vec<_>>()
        );
        assert_eq!(st.range("key", 15, -2), Vec::<String>::new());
        assert_eq!(st.range("key", 0, 1), vec!["1".to_string()]);
        assert_eq!(st.range("key", 1, 1), Vec::<String>::new());
    }
}