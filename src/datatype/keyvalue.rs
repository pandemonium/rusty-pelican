use std::io;

use crate::commands;
use crate::core;
use crate::resp;

pub trait KeyValue {
    fn set(&mut self, key: &str, value: &str);
    fn get(&self, key: &str) -> Result<String, io::Error>;
    fn mget(&self, keys: Vec<&str>) -> Vec<Option<String>>;
}

fn string_prefix(xs: &Vec<String>) -> String {
    xs.into_iter().take(5)
      .map(|s| s.to_string()).collect::<Vec<_>>()
      .join(",")
}

impl KeyValue for core::PersistentState {
    fn set(&mut self, key: &str, value: &str) {
        self.strings.insert(key.to_string(), value.to_string());
    }

    fn get(&self, key: &str) -> Result<String, io::Error> {
        self.strings
            .get(key).map(|s| s.to_string())
            .or_else(|| self.lists.get(key).map(string_prefix))
            .ok_or(io::Error::new(io::ErrorKind::NotFound, key))
    }

    fn mget(&self, keys: Vec<&str>) -> Vec<Option<String>> {
        keys.iter()
            .map(|key| self.get(key).ok())
            .collect()
    }
}

pub fn apply(
    state: &core::State,
    command: commands::StringsApi,
) -> Result<resp::Message, io::Error> {
    match command {
        commands::StringsApi::Set(key, value) => {
            state.for_writing()?.set(&key, &value);
            Ok(resp::Message::SimpleString("OK".to_string()))
        },
        commands::StringsApi::Get(key) =>
            Ok(resp::Message::BulkString(
                state.for_reading()?.get(&key)?                
            )),
        commands::StringsApi::Mget(keys) => {
            let keys = keys.iter().map(|s| s.as_str()).collect();
            let elements = state.for_reading()?.mget(keys).into_iter().map(|value|
                value.map_or(resp::Message::Nil, resp::Message::BulkString)
            );
            Ok(resp::Message::make_array(elements.collect()))
        },
    }
}

#[cfg(test)]
mod tests {
    use crate::core;
    use super::KeyValue;

    #[test]
    fn set() {
        let mut st = core::PersistentState::make();
        st.set("apan:1", "value");
        assert_eq!(st.strings.get("apan:1"), Some(&"value".to_string()));
        assert_eq!(st.strings.len(), 1);
    }

    #[test]
    fn get() {
        let mut st = core::PersistentState::make();
        st.set("apan:1", "value");
        st.set("apan:2", "not_value");
        assert_eq!(st.get("apan:1").map_err(|e| e.to_string()), Ok("value".to_string()));
        assert_eq!(st.get("apan:2").map_err(|e| e.to_string()), Ok("not_value".to_string()));
    }

    #[test]
    fn mget() {
        let mut st = core::PersistentState::make();
        st.set("apan:1", "value");
        st.set("apan:2", "not_value");
        st.set("apan:4", "something else");
        st.lists.insert("apan:5".to_string(), vec![
            "a value".to_string(),
            "two value".to_string(),
        ]);
        assert_eq!(st.strings.len(), 3);
        assert_eq!(
            st.mget(vec!["apan:1", "apan:2", "apan:3", "apan:5"]),
            vec![
                Some("value".to_string()), 
                Some("not_value".to_string()), 
                None,
                Some("a value,two value".to_string())
            ]
        );
    }
}