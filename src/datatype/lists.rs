use std::io;
use std::cmp;
use std::time;
use std::collections;

use crate::core;
use crate::resp;

#[derive(Clone, Debug, PartialEq)]
pub enum ListApi {
    Length(String),
    Append(String, Vec<String>, bool),
    Prepend(String, Vec<String>, bool),
    Set(String, usize, String),
    Range(String, i32, i32),
}

pub trait Lists {
    fn range(&self, key: &str, start: i32, stop: i32) -> Vec<String>;

    /* Replace `to_exixting` with a two-variant. */
    fn append(&mut self, key: &str, element: &str, to_existing: bool) -> usize;
    fn prepend(&mut self, key: &str, element: &str, to_existing: bool) -> usize;

    /* This has a tri-state error condition. More datatypes probably do - solve 
       this with a domain level-error type. */
    fn set_element(&mut self, key: &str, index: usize, element: &str) -> bool;
    fn length(&self, key: &str) -> usize;
}

impl Lists for core::Domain {
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
                self.lists.get(&key.to_string())
                    .unwrap_or(&collections::VecDeque::from(vec![]))
                    .range(effective_start..effective_stop)
                    .map(|s| s.to_string())
                    .collect()
            } else {
                vec![]
            }
        }
    }

    fn append(&mut self, key: &str, element: &str, to_existing: bool) -> usize {
        let xs =
            self.lists
                .entry(key.to_string())
                .and_modify(|xs| xs.push_back(element.to_string()));

        if !to_existing {
            xs.or_insert_with(|| collections::VecDeque::from(vec![element.to_string()]));
        }
        self.expunge_expired(&time::SystemTime::now());
        self.length(key)
    }

    fn prepend(&mut self, key: &str, element: &str, to_existing: bool) -> usize {
        let xs =
            self.lists
                .entry(key.to_string())
                .and_modify(|xs| xs.push_front(element.to_string()));

        if !to_existing {
            xs.or_insert_with(|| collections::VecDeque::from(vec![element.to_string()]));
        }    
        self.length(key)
    }

    fn set_element(&mut self, key: &str, index: usize, element: &str) -> bool {
        matches!(
            self.lists
                .entry(key.to_string())
                .and_modify(|list|
                    if let Some(existing) = list.get_mut(index) {
                        *existing = element.to_string()
                    }
                 ),
            collections::hash_map::Entry::Occupied(_)
        )
    }

    fn length(&self, key: &str) -> usize {
        self.lists
            .get(key).map_or(0, |v| v.len())
    }
}

pub fn apply(
    state:   &core::DomainContext,
    command: core::CommandContext<ListApi>
) -> Result<resp::Message, io::Error> {
    match &*command {
        ListApi::Length(key) =>
            Ok(resp::Message::Integer(
                state.for_reading()?.length(key) as i64
            )),
        ListApi::Append(key, elements, to_existing) => {
            state.apply_transaction(&command, |data| {
                let new_length = elements.iter().fold(0, |_, element| {
                    data.append(key, element, *to_existing)
                });
                resp::Message::Integer(new_length as i64)
            })
        },
        ListApi::Prepend(key, elements, to_existing) => {
            state.apply_transaction(&command, |data| {
                let new_length = elements.iter().fold(0, |_, element| {
                    data.prepend(key, element, *to_existing)
                });
                resp::Message::Integer(new_length as i64)
            })
        },
        ListApi::Set(key, index, element) => {
            state.apply_transaction(&command, |data| {
                if data.set_element(key, *index, element) {
                    resp::Message::SimpleString("OK".to_string())
                } else {
                    resp::Message::Error {
                        prefix: resp::ErrorPrefix::Err,
                        message: "Index out of range".to_string()
                    }
                }
            })
        },
        ListApi::Range(key, start, stop) =>
            Ok(resp::Message::make_bulk_array(
                state.for_reading()?.range(key, *start, *stop).as_slice()
            )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use crate::core;
    use crate::ttl;
    use crate::tx_log;
    use super::Lists;

    fn make_domain() -> Result<core::Domain, io::Error> {
        Ok(tx_log::LoggedTransactions::new(
            ttl::Lifetimes::new(core::Dataset::empty())
        )?)
    }

    #[test]
    fn adding() {
        let mut st = make_domain().unwrap();
        assert_eq!(st.length("key"), 0);
        st.append("key", "1", false);
        st.append("key", "2", false);
        st.prepend("key", "3", false);
        assert_eq!(st.length("key"), 3);
        assert_eq!(st.lists.len(), 1);
        st.prepend("key2", "1", false);
        st.append("key2", "2", false);
        assert_eq!(st.length("key"), 3);
        assert_eq!(st.length("key2"), 2);
        assert_eq!(st.lists.len(), 2);
        assert_eq!(st.lists.get("key"), Some(&VecDeque::from([
            "3".to_string(), "1".to_string(), "2".to_string()
        ])));
        assert_eq!(st.lists.get("key2"), Some(&VecDeque::from([
            "1".to_string(), "2".to_string()
        ])));
    }

    #[test]
    fn add_to_existing() {
        let mut st = make_domain().unwrap();
        assert_eq!(st.append("key", "element", true), 0);
        assert_eq!(st.append("key", "element", false), 1);
        assert_eq!(st.append("key", "element", true), 2);
        assert_eq!(st.prepend("key2", "element", true), 0);
        assert_eq!(st.prepend("key2", "element", false), 1);
        assert_eq!(st.prepend("key2", "element", true), 2);
    }

    #[test]
    fn set() {
        let mut st = make_domain().unwrap();
        assert_eq!(st.set_element("key", 0, "element3"), false);
        st.append("key", "element2", false);
        assert_eq!(st.set_element("key", 0, "element"), true);
        assert_eq!(st.range("key", 0, 10), vec!["element".to_string()]);
    }

    #[test]
    fn range() {
        let mut st = make_domain().unwrap();
        
        for i in 1..10 {
            st.append("key", &i.to_string(), false);
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