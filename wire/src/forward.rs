// Frames the managed backend consumes on behalf of a client. The SDK never
// sends these directly. They live here so the encoding has one definition and
// the golden corpus pins them like every other frame.

use serde::{Deserialize, Serialize};

/// A forwarded managed query. Carries the authenticated identity the SDK cannot
/// set itself, plus the opaque request the SDK sent. CBOR-encoded, named fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardedQuery {
    /// Authenticated identity. Audit today, per-user stream scoping later.
    /// Trusted: set by the server, not the client.
    pub user_id: u32,
    /// Originating client id, for logging and audit only.
    pub client_id: u128,
    /// `gen_ai.conversation.id` echoed for the audit log, never used to route.
    pub correlation: Option<String>,
    /// Opaque CBOR `QueryEnvelope` from the SDK, not decoded in transit.
    #[serde(with = "crate::encoding::bin_bytes")]
    pub query_envelope: Vec<u8>,
}

/// A forwarded keyed managed command (registry browse, key-value, forks). Unlike
/// `ForwardedQuery`, several op kinds share one path, so the frame carries
/// `command_code` and the backend dispatches on it. `payload` is the opaque
/// CBOR request the SDK sent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardedCommand {
    /// Authenticated identity, set by the server. The key-value store scopes its
    /// rows by this `user_id`, and the SDK cannot set it.
    pub user_id: u32,
    /// Originating client id, for logging and audit only.
    pub client_id: u128,
    /// `gen_ai.conversation.id` echoed for the audit log, never used to route.
    pub correlation: Option<String>,
    /// Set by the server, like `user_id`: true when the caller holds a global
    /// read permission. Widens the read-side ops (key-value scan/namespaces,
    /// fork list) to every user's rows, while writes stay scoped to `user_id`
    /// regardless. Defaults to false for a frame from an older server that
    /// does not set it.
    #[serde(default)]
    pub read_all: bool,
    /// The managed command code (browse, key-value, or fork block). The backend
    /// dispatches on it.
    pub command_code: u32,
    /// Opaque CBOR request the SDK sent, not decoded in transit.
    #[serde(with = "crate::encoding::bin_bytes")]
    pub payload: Vec<u8>,
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use super::*;
    use crate::framing::{decode_named, encode_named};

    // A contiguous byte-string run inside the encoded frame proves the opaque
    // field rode as a CBOR byte string (major type 2), not an array of ints.
    // The values are all >= 0x18, which an array would have to widen to two
    // bytes each, so the byte string is also strictly smaller.
    fn contains_byte_string(frame: &[u8], len: u8, fill: u8) -> bool {
        let head = 0x40 | len; // byte string, length in the low 5 bits
        let mut want = vec![head];
        want.extend(std::iter::repeat_n(fill, len as usize));
        frame.windows(want.len()).any(|w| w == want.as_slice())
    }

    #[test]
    fn given_forwarded_query_when_encoded_then_envelope_rides_as_a_byte_string() {
        let frame = encode_named(&ForwardedQuery {
            user_id: 7,
            client_id: 42,
            correlation: Some("conv-1".to_owned()),
            query_envelope: vec![0x90; 5],
        })
        .expect("encodes");
        assert!(
            contains_byte_string(&frame, 5, 0x90),
            "query_envelope must encode as a CBOR byte string, got {frame:02x?}"
        );
    }

    #[test]
    fn given_forwarded_command_when_encoded_then_payload_rides_as_a_byte_string() {
        let frame = encode_named(&ForwardedCommand {
            user_id: 7,
            client_id: 42,
            correlation: None,
            read_all: true,
            command_code: 1_000_000,
            payload: vec![0x90; 5],
        })
        .expect("encodes");
        assert!(
            contains_byte_string(&frame, 5, 0x90),
            "payload must encode as a CBOR byte string, got {frame:02x?}"
        );
    }

    #[test]
    fn given_forwarded_frames_when_round_tripped_then_should_preserve_fields() {
        let query = ForwardedQuery {
            user_id: 1,
            client_id: u128::MAX,
            correlation: Some("c".to_owned()),
            query_envelope: vec![0xff, 0x00, 0x18, 0x7f],
        };
        let back: ForwardedQuery =
            decode_named(&encode_named(&query).expect("encodes")).expect("decodes");
        assert_eq!(back, query);

        let command = ForwardedCommand {
            user_id: 2,
            client_id: 9,
            correlation: None,
            read_all: false,
            command_code: 42,
            payload: vec![0xde, 0xad, 0xbe, 0xef],
        };
        let back: ForwardedCommand =
            decode_named(&encode_named(&command).expect("encodes")).expect("decodes");
        assert_eq!(back, command);
    }
}
