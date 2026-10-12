use serde::{Deserialize, Serialize};

pub const MANAGED_REQUEST_VERSION: u32 = 1;

/// Position of one applied mutation on the KV mutation topic: the fold
/// coordinate a barriered read waits for. A lease grant or renewal returns the
/// position at which the answering fold applied it, and a
/// [`crate::kv::KvGet`] carrying it as `min_position` must not be answered by
/// a fold that has not applied that position yet. Positions compare only
/// within one `topic_generation`: a generation mismatch is fail-closed
/// ([`crate::kv::KvError::Stale`]), never an implicit reset.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationPosition {
    pub topic_generation: u64,
    pub partition: u32,
    pub offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedRequestEnvelope {
    pub v: u32,
    pub operation_id: u128,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub payload: Vec<u8>,
}

impl ManagedRequestEnvelope {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.v != MANAGED_REQUEST_VERSION {
            return Err("unsupported managed request version");
        }
        if self.operation_id == 0 {
            return Err("managed request operation id must not be zero");
        }
        Ok(())
    }
}

/// A managed surface with its own mutation log and fold.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSurface {
    Kv,
    Fork,
    Graph,
}

impl PositionSurface {
    /// The surface a managed read or mutation acts on, or `None` for a
    /// command outside the three mutation logs.
    pub const fn of_code(code: u32) -> Option<Self> {
        use crate::codes::{
            AGDX_FORK_CREATE_CODE, AGDX_FORK_DELETE_CODE, AGDX_FORK_LIST_CODE,
            AGDX_FORK_PROMOTE_CODE, AGDX_FORK_PUT_CODE, AGDX_GRAPH_NEIGHBORS_CODE,
            AGDX_GRAPH_QUERY_CODE, AGDX_GRAPH_UPSERT_CODE, AGDX_KV_CAS_CODE,
            AGDX_KV_CAS_FENCED_CODE, AGDX_KV_COPY_CODE, AGDX_KV_DELETE_CODE,
            AGDX_KV_DELETE_MANY_CODE, AGDX_KV_EXISTS_CODE, AGDX_KV_EXPIRE_CODE, AGDX_KV_GET_CODE,
            AGDX_KV_LEASE_CODE, AGDX_KV_LEASE_RENEW_CODE, AGDX_KV_MOVE_CODE,
            AGDX_KV_NAMESPACES_CODE, AGDX_KV_PATCH_CODE, AGDX_KV_RELEASE_CODE, AGDX_KV_SCAN_CODE,
            AGDX_KV_SET_CODE,
        };
        match code {
            AGDX_KV_GET_CODE
            | AGDX_KV_SCAN_CODE
            | AGDX_KV_NAMESPACES_CODE
            | AGDX_KV_EXISTS_CODE
            | AGDX_KV_SET_CODE
            | AGDX_KV_CAS_CODE
            | AGDX_KV_DELETE_CODE
            | AGDX_KV_DELETE_MANY_CODE
            | AGDX_KV_EXPIRE_CODE
            | AGDX_KV_PATCH_CODE
            | AGDX_KV_COPY_CODE
            | AGDX_KV_MOVE_CODE
            | AGDX_KV_CAS_FENCED_CODE
            | AGDX_KV_LEASE_CODE
            | AGDX_KV_LEASE_RENEW_CODE
            | AGDX_KV_RELEASE_CODE => Some(Self::Kv),
            AGDX_FORK_LIST_CODE
            | AGDX_FORK_CREATE_CODE
            | AGDX_FORK_DELETE_CODE
            | AGDX_FORK_PROMOTE_CODE
            | AGDX_FORK_PUT_CODE => Some(Self::Fork),
            AGDX_GRAPH_QUERY_CODE | AGDX_GRAPH_NEIGHBORS_CODE | AGDX_GRAPH_UPSERT_CODE => {
                Some(Self::Graph)
            }
            _ => None,
        }
    }
}

/// A managed read sent with a consistency requirement. Unknown fields are
/// refused, so no plain read request is ever taken for this frame.
///
/// With `min_position`, the answering node serves the read only once its fold
/// for the read's surface has applied that position. A client sends the
/// highest position it has seen for the surface, so a read never misses its
/// own earlier writes or goes back behind an earlier read, whichever node
/// answers. With `log_head`, the node first waits until its fold has applied
/// every record the surface's log held when the read arrived. A client asks
/// for it after its managed requests moved to another node, when a write whose
/// reply was lost may sit on the log. A node that does not get there within
/// its wait bound answers `Stale`, never older state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedReadEnvelope {
    pub v: u32,
    /// The read request the client would otherwise send on its own.
    #[serde(with = "crate::encoding::bin_bytes")]
    pub read: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_position: Option<MutationPosition>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub log_head: bool,
    /// For a session read: the session fold must have applied each of these
    /// positions that still names a live, registered source partition of the
    /// read's stream. Positions of other streams are ignored.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub min_sessions: Vec<SessionPosition>,
}

/// How far a node's session fold has applied one partition of a session
/// source. Positions compare only within one stream and topic generation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionPosition {
    pub stream_id: u32,
    /// Creation time of the stream incarnation, in microseconds.
    pub stream_generation: u64,
    pub topic: String,
    /// Creation time of the topic incarnation, in microseconds.
    pub topic_generation: u64,
    pub partition_id: u32,
    /// The last offset the fold applied.
    pub offset: u64,
}

/// The positions a node's folds had applied when it answered, per surface.
/// `None` for a surface whose fold has applied nothing yet. `sessions` covers
/// the stream of a session read and is empty for every other reply.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppliedFrontier {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kv: Option<MutationPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork: Option<MutationPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<MutationPosition>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sessions: Vec<SessionPosition>,
}

/// The reply to a managed request sent by an SDK client: the surface reply,
/// plus the frontier the answering node had applied once it computed that
/// reply. After a mutation the frontier covers the mutation itself, and after
/// a read it covers every write the read could see. The client keeps the
/// highest position per surface and sends it back as
/// [`ManagedReadEnvelope::min_position`]. Unknown fields are refused, so no
/// surface reply is ever taken for this frame.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedReplyEnvelope {
    pub v: u32,
    #[serde(default)]
    pub frontier: AppliedFrontier,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub reply: Vec<u8>,
}

#[expect(clippy::trivially_copy_pass_by_ref, reason = "serde skip predicate")]
fn is_false(value: &bool) -> bool {
    !*value
}

/// One managed command stored on a mutation topic. `payload` is the canonical
/// typed request encoded with the normal wire framing for `command_code`, so
/// append and fold share one request schema instead of maintaining a second
/// mutation representation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MutationCommandEnvelope {
    pub v: u32,
    pub operation_id: u128,
    /// Authenticated principal stamped before the command enters the log.
    pub actor_user_id: u32,
    pub deployment_id: u64,
    pub timestamp_micros: u64,
    pub command_code: u32,
    /// Trusted stream identity carried through durable replay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<crate::forward::ForwardedScope>,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub payload: Vec<u8>,
}

impl MutationCommandEnvelope {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.v == 0 {
            return Err("mutation command version must not be zero");
        }
        if self.operation_id == 0 {
            return Err("mutation command operation id must not be zero");
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use super::*;
    use crate::codes::{AGDX_KV_SET_CODE, KV_OP_VERSION};
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_a_read_envelope_when_round_tripped_then_should_keep_the_inner_read() {
        let get = crate::kv::KvGet {
            v: KV_OP_VERSION,
            namespace: "orders".to_owned(),
            key: b"k".to_vec(),
            if_none_match: None,
            min_position: None,
        };
        let read = encode_named(&get).expect("encodes");
        let envelope = ManagedReadEnvelope {
            v: MANAGED_REQUEST_VERSION,
            read: read.clone(),
            min_position: Some(MutationPosition {
                topic_generation: 3,
                partition: 0,
                offset: 41,
            }),
            log_head: true,
            min_sessions: vec![SessionPosition {
                stream_id: 2,
                stream_generation: 10,
                topic: "agent.sessions".to_owned(),
                topic_generation: 11,
                partition_id: 1,
                offset: 7,
            }],
        };
        let decoded: ManagedReadEnvelope =
            decode_named(&encode_named(&envelope).expect("encodes")).expect("decodes");
        assert_eq!(decoded, envelope);
        // A plain read never decodes as the barrier frame, so an unwrapped
        // request is never mistaken for a barriered one.
        assert!(decode_named::<ManagedReadEnvelope>(&read).is_err());
        let managed = encode_named(&ManagedRequestEnvelope {
            v: MANAGED_REQUEST_VERSION,
            operation_id: 9,
            payload: read,
        })
        .expect("encodes");
        assert!(decode_named::<ManagedReadEnvelope>(&managed).is_err());
    }

    #[test]
    fn given_a_reply_envelope_when_round_tripped_then_should_keep_the_reply_and_refuse_plain_replies()
     {
        let reply =
            encode_named(&crate::kv::KvReply::Ok(crate::kv::KvOutcome::Written)).expect("encodes");
        let envelope = ManagedReplyEnvelope {
            v: MANAGED_REQUEST_VERSION,
            frontier: AppliedFrontier {
                kv: Some(MutationPosition {
                    topic_generation: 1,
                    partition: 0,
                    offset: 7,
                }),
                fork: None,
                graph: None,
                sessions: Vec::new(),
            },
            reply: reply.clone(),
        };
        let decoded: ManagedReplyEnvelope =
            decode_named(&encode_named(&envelope).expect("encodes")).expect("decodes");
        assert_eq!(decoded, envelope);
        assert!(decode_named::<ManagedReplyEnvelope>(&reply).is_err());
        let refusal = encode_named(&crate::result::CommandError::new(
            crate::result::ResultCode::Stale,
            "behind",
        ))
        .expect("encodes");
        assert!(decode_named::<ManagedReplyEnvelope>(&refusal).is_err());
    }

    #[test]
    fn given_command_codes_when_classified_then_should_name_their_surface() {
        use crate::codes::{
            AGDX_FORK_LIST_CODE, AGDX_GRAPH_UPSERT_CODE, AGDX_KV_LEASE_CODE, AGDX_QUERY_CODE,
            AGDX_SESSION_GET_CODE,
        };
        assert_eq!(
            PositionSurface::of_code(AGDX_KV_LEASE_CODE),
            Some(PositionSurface::Kv)
        );
        assert_eq!(
            PositionSurface::of_code(AGDX_FORK_LIST_CODE),
            Some(PositionSurface::Fork)
        );
        assert_eq!(
            PositionSurface::of_code(AGDX_GRAPH_UPSERT_CODE),
            Some(PositionSurface::Graph)
        );
        assert_eq!(PositionSurface::of_code(AGDX_SESSION_GET_CODE), None);
        assert_eq!(PositionSurface::of_code(AGDX_QUERY_CODE), None);
    }

    #[test]
    fn given_a_command_envelope_when_round_tripped_then_should_preserve_request_bytes() {
        let envelope = MutationCommandEnvelope {
            v: KV_OP_VERSION,
            operation_id: 42,
            actor_user_id: 7,
            deployment_id: 3,
            timestamp_micros: 1_700_000_000_000_000,
            command_code: AGDX_KV_SET_CODE,
            scope: None,
            payload: vec![0, 1, 2, 255],
        };
        let bytes = encode_named(&envelope).expect("encodes");
        let back: MutationCommandEnvelope = decode_named(&bytes).expect("decodes");
        back.validate().expect("the envelope is valid");
        assert_eq!(back.v, KV_OP_VERSION);
        assert_eq!(back.operation_id, 42);
        assert_eq!(back.actor_user_id, 7);
        assert_eq!(back.deployment_id, 3);
        assert_eq!(back.timestamp_micros, 1_700_000_000_000_000);
        assert_eq!(back.command_code, AGDX_KV_SET_CODE);
        assert_eq!(back.payload, vec![0, 1, 2, 255]);
    }

    #[test]
    fn given_a_managed_request_when_round_tripped_then_should_preserve_operation_identity() {
        let request = ManagedRequestEnvelope {
            v: MANAGED_REQUEST_VERSION,
            operation_id: u128::MAX,
            payload: vec![4, 3, 2, 1],
        };

        let bytes = encode_named(&request).expect("managed request encodes");
        let back: ManagedRequestEnvelope = decode_named(&bytes).expect("managed request decodes");

        assert_eq!(back, request);
        back.validate().expect("the request is valid");
    }

    #[test]
    fn given_a_zero_operation_identity_when_validated_then_should_reject_it() {
        let request = ManagedRequestEnvelope {
            v: MANAGED_REQUEST_VERSION,
            operation_id: 0,
            payload: Vec::new(),
        };
        let command = MutationCommandEnvelope {
            v: KV_OP_VERSION,
            operation_id: 0,
            timestamp_micros: 1,
            actor_user_id: 7,
            deployment_id: 3,
            command_code: AGDX_KV_SET_CODE,
            scope: None,
            payload: Vec::new(),
        };

        assert!(request.validate().is_err());
        assert!(command.validate().is_err());
    }
}
