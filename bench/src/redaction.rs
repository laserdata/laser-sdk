use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

const REDACTED: &str = "[REDACTED]";
const SENSITIVE_FRAGMENTS: &[&str] = &[
    "credential",
    "password",
    "secret",
    "token",
    "private_key",
    "signing_key",
];

#[derive(Clone, Debug)]
pub struct Redactor {
    allowed_keys: BTreeSet<String>,
}

impl Redactor {
    #[must_use]
    pub fn new(allowed_keys: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            allowed_keys: allowed_keys.into_iter().map(Into::into).collect(),
        }
    }

    #[must_use]
    pub fn sanitize_object(&self, value: &Value) -> Value {
        let Some(object) = value.as_object() else {
            return Value::Object(Map::new());
        };
        let sanitized = object
            .iter()
            .filter(|(key, _)| self.allowed_keys.contains(*key))
            .map(|(key, value)| (key.clone(), sanitize_entry(key, value)))
            .collect();
        Value::Object(sanitized)
    }

    #[must_use]
    pub fn capture_environment(
        &self,
        environment: impl IntoIterator<Item = (String, String)>,
    ) -> BTreeMap<String, String> {
        environment
            .into_iter()
            .filter(|(key, _)| self.allowed_keys.contains(key))
            .map(|(key, value)| {
                let sanitized = if is_sensitive(&key) {
                    REDACTED.to_owned()
                } else {
                    sanitize_string(&value)
                };
                (key, sanitized)
            })
            .collect()
    }
}

fn sanitize_entry(key: &str, value: &Value) -> Value {
    if is_sensitive(key) {
        return Value::String(REDACTED.to_owned());
    }
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(nested_key, nested_value)| {
                    (nested_key.clone(), sanitize_entry(nested_key, nested_value))
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| sanitize_entry(key, value))
                .collect(),
        ),
        Value::String(value) => Value::String(sanitize_string(value)),
        value => value.clone(),
    }
}

fn is_sensitive(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    SENSITIVE_FRAGMENTS
        .iter()
        .any(|fragment| key.contains(fragment))
}

fn sanitize_string(value: &str) -> String {
    let Some(scheme_end) = value.find("://") else {
        return value.to_owned();
    };
    let authority_start = scheme_end + 3;
    let authority_end = value[authority_start..]
        .find('/')
        .map_or(value.len(), |offset| authority_start + offset);
    let authority = &value[authority_start..authority_end];
    let Some(at) = authority.rfind('@') else {
        return value.to_owned();
    };
    format!(
        "{}{}{}",
        &value[..authority_start],
        REDACTED,
        &value[authority_start + at..]
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn given_unallowlisted_and_sensitive_values_when_sanitized_then_should_not_leak_them() {
        let redactor = Redactor::new(["server_url", "password", "threads"]);
        let sanitized = redactor.sanitize_object(&json!({
            "server_url": "tcp://iggy:secret@127.0.0.1:8090/path",
            "password": "secret",
            "threads": 4,
            "unrelated": "private"
        }));
        assert_eq!(sanitized["password"], REDACTED);
        assert_eq!(
            sanitized["server_url"],
            "tcp://[REDACTED]@127.0.0.1:8090/path"
        );
        assert_eq!(sanitized["threads"], 4);
        assert!(sanitized.get("unrelated").is_none());
        assert!(!sanitized.to_string().contains("secret"));
    }

    #[test]
    fn given_environment_allowlist_when_captured_then_should_drop_every_other_variable() {
        let redactor = Redactor::new(["RUST_LOG", "AUTH_TOKEN"]);
        let captured = redactor.capture_environment([
            ("RUST_LOG".to_owned(), "warn".to_owned()),
            ("AUTH_TOKEN".to_owned(), "secret".to_owned()),
            ("HOME".to_owned(), "/private/home".to_owned()),
        ]);
        assert_eq!(captured.get("RUST_LOG").map(String::as_str), Some("warn"));
        assert_eq!(
            captured.get("AUTH_TOKEN").map(String::as_str),
            Some(REDACTED)
        );
        assert!(!captured.contains_key("HOME"));
    }
}
