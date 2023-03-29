use std::ops::{Deref, DerefMut};
use std::collections;
use std::time;
use serde::{Deserialize, Serialize};

pub trait Expungeable {
    fn expunge(&mut self, id: &str);
}

#[derive(Deserialize, Serialize)]
pub struct Lifetimes<Underlying: Expungeable + Serialize> {
    expires:    collections::BTreeMap<time::SystemTime, String>,
    ttls:       collections::HashMap<String, time::SystemTime>,
    underlying: Underlying,
}

impl <A: Expungeable + Serialize> Deref for Lifetimes<A> {
    type Target = A;
    fn deref(&self) -> &Self::Target {
        &self.underlying
    }
}

impl <A: Expungeable + Serialize> DerefMut for Lifetimes<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.underlying
    }
}

impl <Underlying: Expungeable + Serialize> Lifetimes<Underlying> {
    pub fn new(underlying: Underlying) -> Self {
        Self {
            expires:    collections::BTreeMap::new(),
            ttls:       collections::HashMap::new(),
            underlying,
        }
    }

    pub fn expunge_expired(&mut self, now: &time::SystemTime) {
        while let Some((expires, key)) = self.expires.pop_first() {
            let expires_at = self.ttls.get(&key).unwrap_or(&expires);
            if expires_at < now {
                println!("expunge_expired: key={:?}", key);
                self.underlying.expunge(&key);
            } else {
                self.expires.insert(*expires_at, key);
                break;
            }
        }
    }

    pub fn register_ttl(
        &mut self, 
        key: &str,
        now: time::SystemTime, 
        ttl: time::Duration
    ) {
        let at = now + ttl;
        println!("register_ttl: ttl {:?} for {:?}", at, key);
        self.ttls.entry(key.to_string())
            .and_modify(|expires_at| *expires_at = at)
            .or_insert(at);
        self.expires.insert(at, key.to_string());
        println!("register_ttl: ttls={:?}, expires={:?}", &self.ttls, &self.expires);
    }

    pub fn ttl_remaining(
        &self, 
        key: &str,
        now: &time::SystemTime
    ) -> Option<time::Duration> {
        self.ttls.get(key).and_then(|expires_at| expires_at.duration_since(*now).ok())
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use super::*;
    use crate::core;
    use crate::datatype::keyvalue::*;
    use crate::tx_log;
    use crate::ttl;

    fn make_domain() -> Result<core::Domain, io::Error> {
        Ok(tx_log::LoggedTransactions::new(
            ttl::Lifetimes::new(core::Dataset::empty())
        )?)
    }

    #[test]
    fn register_ttl() {
        let mut st = make_domain().unwrap();
        let now = time::SystemTime::now();
        assert_eq!(st.ttl_remaining(&"key".to_string(), &now), None);
        st.register_ttl(&"key".to_string(), now, time::Duration::from_secs(1));
        assert_eq!(
            st.ttl_remaining(&"key".to_string(), &now), 
            Some(time::Duration::from_secs(1))
        );
    }

    #[test]
    fn expires_the_right_one() {
        let mut st = make_domain().unwrap();
        let now = time::SystemTime::now();
        st.set("key", "value");
        st.register_ttl(&"key".to_string(), now, time::Duration::from_secs(0));
        assert_eq!(st.get("key").ok(), Some("value".to_string()));
        st.set("key2", "value");
        assert_eq!(st.get("key").ok(), None);
        assert_eq!(st.get("key2").ok(), Some("value".to_string()));
    }
}