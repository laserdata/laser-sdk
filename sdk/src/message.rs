use crate::stream::Headers;
use crate::types::MessageId;
use iggy::prelude::IggyMessage;

/// A generic message read off the log: raw payload, the source `MessageId`, and
/// the user headers with each value's wire type. No agentic decoding (no
/// `Provenance`). The agent layer reconstructs that on top from the same
/// `headers`.
#[derive(Clone, Debug)]
pub struct Message {
    /// The raw message body. Owned `Vec<u8>` so the public API never leaks the
    /// `bytes` crate.
    pub payload: Vec<u8>,
    /// Where the message sits on the log (partition + offset).
    pub id: MessageId,
    /// Exact Apache Iggy user headers, each value with its wire type. Empty
    /// when the header block does not decode.
    pub headers: Headers,
}

impl Message {
    /// Decode the payload as JSON.
    pub fn json<T: serde::de::DeserializeOwned>(&self) -> Result<T, crate::LaserError> {
        serde_json::from_slice(&self.payload)
            .map_err(|error| crate::LaserError::Codec(error.to_string()))
    }

    pub(crate) fn of(partition_id: u32, message: IggyMessage) -> Self {
        Self {
            id: MessageId::new(partition_id, message.header.offset),
            headers: message
                .user_headers_map()
                .ok()
                .flatten()
                .unwrap_or_default(),
            payload: message.payload.to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::{HeaderKey, HeaderValue};
    use std::str::FromStr;

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

    #[test]
    fn given_typed_user_headers_when_a_message_is_read_then_should_keep_every_value_with_its_type()
    {
        let mut headers = Headers::new();
        headers.insert(
            HeaderKey::from_str("kind").expect("valid key"),
            HeaderValue::from_str("reading").expect("valid value"),
        );
        headers.insert(
            HeaderKey::from_str("type").expect("valid key"),
            HeaderValue::from(7_u16),
        );
        let polled = IggyMessage::builder()
            .payload(bytes::Bytes::from_static(b"body"))
            .user_headers(headers.clone())
            .build()
            .expect("a valid message");

        let message = Message::of(3, polled);

        assert_eq!(message.id, MessageId::new(3, 0));
        assert_eq!(message.headers, headers, "every typed header is kept");
    }

    fn message(payload: &[u8]) -> Message {
        Message {
            payload: payload.to_vec(),
            id: MessageId::new(0, 7),
            headers: Headers::new(),
        }
    }
}
