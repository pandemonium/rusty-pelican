use std::ops::{Deref, DerefMut};
use std::hash;
use std::collections;
use std::time;
use std::fmt;
use std::io;

pub trait Expungeable {
    type Key: PartialEq + Eq + hash::Hash + Clone + fmt::Debug;
    fn expunge(&mut self, id: &Self::Key);
}

pub struct Lifetimes<Underlying: Expungeable> {
    expires:    collections::BTreeMap<time::Instant, Underlying::Key>,
    ttls:       collections::HashMap<Underlying::Key, time::Instant>,
    underlying: Underlying,
}

impl <A: Expungeable> Deref for Lifetimes<A> {
    type Target = A;
    fn deref(&self) -> &Self::Target {
        &self.underlying
    }
}

impl <A: Expungeable> DerefMut for Lifetimes<A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.underlying
    }
}

impl <Underlying: Expungeable> Lifetimes<Underlying> {
    pub fn new(underlying: Underlying) -> Self {
        Self {
            expires:    collections::BTreeMap::new(),
            ttls:       collections::HashMap::new(),
            underlying: underlying,
        }
    }

    pub fn expunge_expired(&mut self, now: &time::Instant) {
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
        key: &Underlying::Key, 
        now: time::Instant, 
        ttl: time::Duration
    ) {
        let at = now + ttl;
        println!("register_ttl: ttl {:?} for {:?}", at, key);
        self.ttls.entry(key.clone())
            .and_modify(|expires_at| *expires_at = at)
            .or_insert(at);
        self.expires.insert(at, key.clone());
        println!("register_ttl: ttls={:?}, expires={:?}", &self.ttls, &self.expires);
    }

    pub fn ttl_remaining(
        &self, 
        key: &Underlying::Key, 
        now: &time::Instant
    ) -> Option<time::Duration> {
        self.ttls.get(key).map(|expires_at| expires_at.duration_since(*now))
    }
}

#[cfg(test)]
mod tests {
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
        let now = time::Instant::now();
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
        let now = time::Instant::now();
        st.set("key", "value");
        st.register_ttl(&"key".to_string(), now, time::Duration::from_secs(0));
        assert_eq!(st.get("key").ok(), Some("value".to_string()));
        st.set("key2", "value");
        assert_eq!(st.get("key").ok(), None);
        assert_eq!(st.get("key2").ok(), Some("value".to_string()));
    }
}