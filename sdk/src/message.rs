use crate::types::MessageId;
use std::collections::BTreeMap;

/// A generic message read off the log: raw payload, the source `MessageId`, and
/// the user-headers decoded as strings. No agentic decoding (no `Provenance`).
/// The agent layer reconstructs that on top from the same `headers`.
#[derive(Clone, Debug)]
pub struct Message {
    /// The raw message body. Owned `Vec<u8>` so the public API never leaks the
    /// `bytes` crate.
    pub payload: Vec<u8>,
    /// Where the message sits on the log (partition + offset).
    pub id: MessageId,
    /// User headers decoded to strings (non-UTF-8 entries dropped).
    pub headers: BTreeMap<String, String>,
}

impl Message {
    /// Decode the payload as JSON.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, crate::LaserError> {
        serde_json::from_slice(&self.payload)
            .map_err(|error| crate::LaserError::Codec(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(payload: &[u8]) -> Message {
        Message {
            payload: payload.to_vec(),
            id: MessageId::new(0, 7),
            headers: BTreeMap::new(),
        }
    }

    #[test]
    fn given_a_json_payload_when_decoding_then_should_return_the_value() {
        let value: serde_json::Value = message(br#"{"n":1}"#).json().expect("json");
        assert_eq!(value, serde_json::json!({"n": 1}));
    }

    #[test]
    fn given_a_non_json_payload_when_decoding_then_should_return_a_codec_error() {
        let error = message(b"\xff").json::<serde_json::Value>().unwrap_err();
        assert!(matches!(error, crate::LaserError::Codec(_)));
    }
}
