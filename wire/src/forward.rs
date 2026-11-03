// Frames the managed backend consumes on behalf of a client. The SDK never
// sends these directly. They live here so the encoding has one definition and
// the golden corpus pins them like every other frame.

use serde::{Deserialize, Serialize};

/// Who asked for a request the managed backend records, present only when a
/// client asked. A record the backend or the streaming server issues on its own
/// carries none.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Caller {
    /// The authenticated user. The root user is id zero.
    pub user_id: u32,
    /// The agent the caller's connection declared in its presence. A claim the
    /// client makes, never verified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<crate::agent::AgentId>,
    /// The end user an intermediary wrote for, as it named that user in
    /// [`ON_BEHALF_OF_HEADER`](crate::http::ON_BEHALF_OF_HEADER). A claim the
    /// backend records, never verified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_behalf_of: Option<String>,
}

/// Trusted stream identity stamped by the server before forwarding a session request.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardedScope {
    pub stream_id: u32,
    pub stream: String,
    pub stream_created_at_micros: u64,
}

/// A forwarded managed query. Carries the authenticated identity the SDK cannot
/// set itself, plus the opaque request the SDK sent. CBOR-encoded, named fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardedQuery {
    /// Authenticated identity. Audit today, per-user stream scoping later.
    /// Trusted: set by the server, not the client.
    pub user_id: u32,
    /// Originating client id, for logging and audit only. Never zero for a
    /// client, zero on a request the server issues itself.
    pub client_id: u128,
    /// `gen_ai.conversation.id` echoed for the audit log, never used to route.
    pub correlation: Option<String>,
    /// Opaque CBOR `QueryEnvelope` from the SDK, not decoded in transit.
    #[serde(with = "crate::encoding::bin_bytes")]
    pub query_envelope: Vec<u8>,
    /// The caller's effective capability grants, stamped by the server. Empty
    /// (the default, skipped on the wire) when authorization is not in effect.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<crate::authz::Grant>,
    /// The tenancy of the server's replicated authorization state. The
    /// backend refuses a frame whose tenancy differs from its own.
    #[serde(default, skip_serializing_if = "crate::authz::Tenancy::is_deployment")]
    pub tenancy: crate::authz::Tenancy,
    /// The stream of the queried index, stamped once the caller's native read
    /// authority over it was checked. The backend refuses a query of a
    /// stream-scoped index that arrives without this scope or with another.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<ForwardedScope>,
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
    /// The caller's consensus client id, for logging and audit only. Never zero
    /// for a client, zero on a request the server issues itself.
    pub client_id: u128,
    /// `gen_ai.conversation.id` echoed for the audit log, never used to route.
    pub correlation: Option<String>,
    /// Stable identity minted by the originating SDK for a logical managed
    /// mutation. Read-only commands leave it absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<u128>,
    /// Set by the server, like `user_id`: true when the caller holds a global
    /// read permission. Widens the read-side ops (key-value scan/namespaces,
    /// fork list) to every user's rows, while writes stay scoped to `user_id`
    /// regardless.
    #[serde(default)]
    pub read_all: bool,
    /// Server-stamped stream identity. A session command without it fails closed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<ForwardedScope>,
    /// The managed command code (browse, key-value, or fork block). The backend
    /// dispatches on it.
    pub command_code: u32,
    /// Opaque CBOR request the SDK sent, not decoded in transit.
    #[serde(with = "crate::encoding::bin_bytes")]
    pub payload: Vec<u8>,
    /// The caller's effective capability grants, stamped by the server. Empty
    /// (the default, skipped on the wire) when authorization is not in effect.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<crate::authz::Grant>,
    /// Set by the server when the client wrapped this read in a
    /// [`crate::mutation::ManagedReadEnvelope`]. The backend answers only once
    /// its fold has reached the surface's mutation log head.
    #[serde(default, skip_serializing_if = "is_false")]
    pub read_barrier: bool,
    /// Set by the server from [`crate::mutation::ManagedReadEnvelope`]: the
    /// backend answers only once its fold for the read's surface has applied
    /// this position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_position: Option<crate::mutation::MutationPosition>,
    /// Set by the server for a managed request an SDK client sent over the
    /// binary protocol. The backend answers with a
    /// [`crate::mutation::ManagedReplyEnvelope`] carrying its applied
    /// frontier.
    #[serde(default, skip_serializing_if = "is_false")]
    pub positioned_reply: bool,
    /// Set by the server from [`crate::mutation::ManagedReadEnvelope`]: the
    /// session positions a session read must find applied.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub min_sessions: Vec<crate::mutation::SessionPosition>,
    /// The tenancy of the server's replicated authorization state. The
    /// backend refuses a frame whose tenancy differs from its own.
    #[serde(default, skip_serializing_if = "crate::authz::Tenancy::is_deployment")]
    pub tenancy: crate::authz::Tenancy,
    /// Set by the server on a mutation a client sent over the binary protocol:
    /// the agent the calling connection declared in its presence. A claim the
    /// client makes, never verified. Absent over HTTP and on the server's own
    /// requests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<crate::agent::AgentId>,
    /// Set by the server from [`ON_BEHALF_OF_HEADER`](crate::http::ON_BEHALF_OF_HEADER)
    /// on an HTTP write: the end user an intermediary wrote for. A claim, never
    /// verified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_behalf_of: Option<String>,
}

#[expect(clippy::trivially_copy_pass_by_ref, reason = "serde skip predicate")]
fn is_false(value: &bool) -> bool {
    !*value
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
            grants: Vec::new(),
            tenancy: Default::default(),
            scope: None,
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
            read_barrier: false,
            min_position: None,
            positioned_reply: false,
            min_sessions: Vec::new(),
            tenancy: Default::default(),
            user_id: 7,
            client_id: 42,
            correlation: None,
            operation_id: Some(17),
            read_all: true,
            scope: None,
            command_code: 1_000_000,
            payload: vec![0x90; 5],
            grants: Vec::new(),
            agent: None,
            on_behalf_of: None,
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
            grants: Vec::new(),
            tenancy: Default::default(),
            scope: Some(ForwardedScope {
                stream_id: 3,
                stream: "acme".to_owned(),
                stream_created_at_micros: 9,
            }),
        };
        let back: ForwardedQuery =
            decode_named(&encode_named(&query).expect("encodes")).expect("decodes");
        assert_eq!(back, query);

        let command = ForwardedCommand {
            read_barrier: false,
            min_position: None,
            positioned_reply: false,
            min_sessions: Vec::new(),
            tenancy: Default::default(),
            user_id: 2,
            client_id: 9,
            correlation: None,
            operation_id: Some(9),
            read_all: false,
            scope: None,
            command_code: 42,
            payload: vec![0xde, 0xad, 0xbe, 0xef],
            grants: Vec::new(),
            agent: None,
            on_behalf_of: None,
        };
        let back: ForwardedCommand =
            decode_named(&encode_named(&command).expect("encodes")).expect("decodes");
        assert_eq!(back, command);
    }

    #[test]
    fn given_a_root_caller_with_an_agent_when_round_tripped_then_should_keep_both() {
        let caller = Caller {
            user_id: 0,
            agent: Some("orbit-planner".parse().expect("agent id")),
            on_behalf_of: None,
        };

        let back: Caller = decode_named(&encode_named(&caller).expect("encodes")).expect("decodes");

        assert_eq!(back, caller);
    }

    #[test]
    fn given_a_caller_written_for_an_end_user_when_round_tripped_then_should_keep_who_it_was_for() {
        let caller = Caller {
            user_id: 3,
            agent: None,
            on_behalf_of: Some("1793690355287073".to_owned()),
        };

        let back: Caller = decode_named(&encode_named(&caller).expect("encodes")).expect("decodes");

        assert_eq!(back, caller);
    }

    #[test]
    fn given_a_caller_with_an_empty_agent_when_decoded_then_should_be_refused() {
        #[derive(serde::Serialize)]
        struct Unchecked<'a> {
            user_id: u32,
            agent: &'a str,
        }
        let frame = encode_named(&Unchecked {
            user_id: 3,
            agent: "",
        })
        .expect("encodes");

        assert!(decode_named::<Caller>(&frame).is_err());
    }

    #[test]
    fn given_a_forwarded_command_with_an_agent_when_read_without_the_field_then_should_ignore_it() {
        #[derive(Debug, PartialEq, serde::Deserialize)]
        struct BeforeAgent {
            user_id: u32,
            client_id: u128,
            command_code: u32,
        }
        let command = ForwardedCommand {
            read_barrier: false,
            min_position: None,
            positioned_reply: false,
            min_sessions: Vec::new(),
            tenancy: Default::default(),
            user_id: 0,
            client_id: 9,
            correlation: None,
            operation_id: Some(9),
            read_all: false,
            scope: None,
            command_code: 42,
            payload: vec![1],
            grants: Vec::new(),
            agent: Some("orbit-planner".parse().expect("agent id")),
            on_behalf_of: None,
        };

        let before: BeforeAgent =
            decode_named(&encode_named(&command).expect("encodes")).expect("decodes");

        assert_eq!(
            before,
            BeforeAgent {
                user_id: 0,
                client_id: 9,
                command_code: 42,
            }
        );
    }
}
