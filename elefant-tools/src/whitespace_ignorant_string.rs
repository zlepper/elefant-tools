use std::fmt::{Debug, Display};
use std::ops::{Deref, DerefMut};

/// A string that ignores repeated whitespace when comparing equality,
/// while still storing the original string.
#[repr(transparent)]
#[derive(Default, Eq, Clone)]
pub struct WhitespaceIgnorantString {
    inner: String,
}

impl Deref for  WhitespaceIgnorantString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for WhitespaceIgnorantString {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl From<String> for WhitespaceIgnorantString {
    fn from(s: String) -> Self {
        Self { inner: s }
    }
}

impl From<&str> for WhitespaceIgnorantString {
    fn from(s: &str) -> Self {
        Self { inner: s.to_string() }
    }
}

impl From<WhitespaceIgnorantString> for String {
    fn from(s: WhitespaceIgnorantString) -> Self {
        s.inner
    }
}

impl Debug for WhitespaceIgnorantString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.inner, f)
    }
}

impl Display for WhitespaceIgnorantString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.inner, f)
    }
}

impl PartialEq<Self> for WhitespaceIgnorantString {
    fn eq(&self, other: &Self) -> bool {
        self.inner.split_whitespace().collect::<String>() == other.inner.split_whitespace().collect::<String>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a() {
        let s1 = WhitespaceIgnorantString::from("  hello  ".to_string());
        let s2 = WhitespaceIgnorantString::from("hello".to_string());
        assert_eq!(s1, s2);
        assert_eq!(s1.to_string(), "  hello  ");
        assert_eq!(s2.to_string(), "hello");
    }

    #[test]
    fn b() {
        let s1 = WhitespaceIgnorantString::from("hel        lo".to_string());
        let s2 = WhitespaceIgnorantString::from("hel lo".to_string());
        assert_eq!(s1, s2);
        assert_eq!(s1.to_string(), "hel        lo");
        assert_eq!(s2.to_string(), "hel lo");
    }

    #[test]
    fn c() {
        let s1 = WhitespaceIgnorantString::from("  hel        lo  ".to_string());
        let s2 = WhitespaceIgnorantString::from("hel lo".to_string());
        assert_eq!(s1, s2);
    }

    #[test]
    fn d() {
        let s1 = WhitespaceIgnorantString::from("  hel        lo  ".to_string());
        let s2 = WhitespaceIgnorantString::from(" hel lo     ".to_string());
        assert_eq!(s1, s2);
    }

    #[test]
    fn e() {
        let s1 = WhitespaceIgnorantString::from(r#"{"hypertable":"metrics"}"#.to_string());
        let s2 = WhitespaceIgnorantString::from(r#"{"hypertable": "metrics"}"#.to_string());
        assert_eq!(s1, s2);
    }
}