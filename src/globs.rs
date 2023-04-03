pub struct Glob(regex::Regex);

impl Glob {
    pub fn new(pattern: &str) -> Option<Self> {
        let mut buf = String::from(pattern).replace('*', ".+");
        buf.insert(0, '^');
        buf.push('$');

        let re = regex::Regex::new(&buf);
        Some(Self(re.ok()?))
    }

    pub fn matches(&self, candidate: &str) -> bool {
        self.0.is_match(candidate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn over_eager() {
        assert!(!Glob::new("users:*").unwrap().matches("sweden:users:429"));
        assert!(!Glob::new("*:users").unwrap().matches("sweden:users:429"));
    }
}