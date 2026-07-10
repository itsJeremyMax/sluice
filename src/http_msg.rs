use std::collections::BTreeMap;

use base64::Engine;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HttpMsg {
    pub method: String,
    pub path: String,
    pub headers: BTreeMap<String, String>,
    pub body_b64: String,
}

impl HttpMsg {
    pub fn body_bytes(&self) -> Result<Vec<u8>, base64::DecodeError> {
        base64::engine::general_purpose::STANDARD.decode(&self.body_b64)
    }

    pub fn set_body_bytes(&mut self, bytes: &[u8]) {
        self.body_b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
    }

    /// Insert or replace a header, normalizing the name to lowercase.
    pub fn set_header(&mut self, name: &str, value: &str) {
        self.headers
            .insert(name.to_ascii_lowercase(), value.to_string());
    }

    /// Remove a header (case-insensitive). Returns true if present.
    pub fn delete_header(&mut self, name: &str) -> bool {
        self.headers.remove(&name.to_ascii_lowercase()).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_roundtrips_through_base64() {
        let mut m = HttpMsg {
            method: "POST".into(),
            path: "/v1/messages".into(),
            headers: BTreeMap::new(),
            body_b64: String::new(),
        };
        m.set_body_bytes(b"hello");
        assert_eq!(m.body_bytes().unwrap(), b"hello");
    }

    #[test]
    fn set_header_lowercases_name() {
        let mut m = HttpMsg {
            method: "GET".into(),
            path: "/".into(),
            headers: BTreeMap::new(),
            body_b64: String::new(),
        };
        m.set_header("X-Tag", "seen");
        assert_eq!(m.headers.get("x-tag").map(String::as_str), Some("seen"));
    }

    #[test]
    fn delete_header_is_case_insensitive() {
        let mut m = HttpMsg {
            method: "GET".into(),
            path: "/".into(),
            headers: BTreeMap::new(),
            body_b64: String::new(),
        };
        m.set_header("x-tag", "seen");
        assert!(m.delete_header("X-TAG"));
        assert!(m.headers.is_empty());
    }
}
