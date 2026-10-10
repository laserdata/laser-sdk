use crate::agent::{
    AgentId, Budget, ConversationId, CorrelationId, LogPosition, SdkInfo, SessionStatus, TokenUsage,
};
use crate::graph::SourceRef;
use crate::schema::Digest32;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The resource surface used to narrow a session link read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkSurface {
    Memory,
    Kv,
    GraphNode,
    GraphEdge,
    Projection,
    Child,
}

/// The relationship a session has with a linked resource.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkRelation {
    Wrote,
    Recalled,
    Touched,
}

/// Read requests. Every request names the stream before any other field.
pub mod request {
    use super::*;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SessionGet {
        pub stream: String,
        pub id: ConversationId,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SessionList {
        pub stream: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub status: Option<SessionStatus>,
        /// Only sessions in the tree rooted at this session.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub root: Option<ConversationId>,
        /// Only sessions whose label starts with this prefix.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub label_prefix: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub agent: Option<AgentId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub text: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub cursor: Option<String>,
        /// The page size. Zero leaves it to the server.
        pub limit: u32,
        pub want_total: bool,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SessionEvents {
        pub stream: String,
        pub id: ConversationId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub cursor: Option<String>,
        /// The page size. Zero leaves it to the server.
        pub limit: u32,
        pub fixed_frontier: bool,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SessionState {
        pub stream: String,
        pub id: ConversationId,
        /// The most history rows. Zero leaves it to the server.
        pub history_limit: u32,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SessionLinks {
        pub stream: String,
        pub id: ConversationId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub surface: Option<LinkSurface>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SessionSources {
        pub stream: String,
        pub id: ConversationId,
        /// Return only the registered lane identity for a native lane writer.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        pub lane_only: bool,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SessionChanges {
        pub stream: String,
        pub after: u64,
        /// The page size. Zero leaves it to the server.
        pub limit: u32,
    }
}

/// How far the session index has folded one source partition. Offsets are
/// never compared across topics. `topic_generation` is the topic creation time
/// in microseconds, so a recreated topic with a reused id is a different source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceFrontier {
    pub topic_id: u32,
    pub topic_generation: u64,
    pub partition_id: u32,
    /// The last offset folded into the index, absent before the first record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub folded: Option<u64>,
    /// The live partition head when the read probed it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<u64>,
    /// The first offset still retained by the source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retained_from: Option<u64>,
}

/// Why part of a session timeline is missing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapReason {
    /// The source expired these offsets before the index folded them.
    ExpiredBeforeFold,
    /// The index pruned these offsets after the source expired them.
    Pruned,
    /// The session reached its event cap, so these records were counted only.
    Truncated,
    /// The source is being rebuilt after a reset.
    Rebuilding,
    #[serde(other)]
    Unrecognized,
}

/// A missing inclusive offset range on one source partition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceGap {
    pub topic_id: u32,
    pub topic_generation: u64,
    pub partition_id: u32,
    pub from: u64,
    pub to: u64,
    pub reason: GapReason,
}

/// The inclusive offset range on one source partition that holds the payloads
/// of a timeline page. A range is a hint for bounded polls, not a promise that
/// every offset in it belongs to the session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadRange {
    pub topic_id: u32,
    pub topic_generation: u64,
    pub partition_id: u32,
    pub first: u64,
    pub last: u64,
}

/// What the fold did with one state record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StateOutcome {
    /// The patch or snapshot changed the document.
    Applied,
    /// A patch operation failed, so the whole patch was rejected.
    Rejected,
    /// The base revision did not match the applied revision.
    Stale,
    /// The operation id was already applied.
    Duplicate,
    #[serde(other)]
    Unrecognized,
}

/// One entry in the state history of a session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateChange {
    /// The document revision after this record.
    pub revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op_id: Option<String>,
    pub outcome: StateOutcome,
    pub at: LogPosition,
    pub broker_ts: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_digest: Option<Digest32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_digest: Option<Digest32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// Conditions that make a session summary partial.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionFlags {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub label_truncated: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub overflow: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub events_truncated: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub lane_conflict: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub rebuilding: bool,
    /// Liveness is not known yet because the heartbeat tail has not caught
    /// up, so `idle` is not meaningful.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub liveness_unknown: bool,
}

/// The summary of one session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionInfo {
    pub stream: String,
    pub id: ConversationId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<ConversationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root: Option<ConversationId>,
    pub status: SessionStatus,
    pub idle: bool,
    pub over_budget: bool,
    pub pause_requested: bool,
    pub cancel_requested: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_event_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_at: Option<u64>,
    pub events: u64,
    pub model_calls: u64,
    pub tool_calls: u64,
    pub tokens_in: u64,
    pub tokens_out: u64,
    pub cost_micros: u64,
    pub errors: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<Budget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sdk: Option<SdkInfo>,
    #[serde(default)]
    pub flags: SessionFlags,
    /// Work records held while the session was paused and not yet handled.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub held: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frontier: Vec<SourceFrontier>,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}

/// One page of session summaries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPage {
    pub items: Vec<SessionInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub searched: Option<u64>,
    pub truncated: bool,
}

/// One indexed record on a session timeline. The summary never holds prompt
/// text or tool argument values.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionEvent {
    pub at: SourceRef,
    pub session: ConversationId,
    pub broker_ts: u64,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    pub display: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent: Option<AgentId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub addressee: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation: Option<CorrelationId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<LogPosition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<TokenUsage>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub after_end: bool,
    /// The principal whose enrolled key verified this record's signature.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verified_actor: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub summary: BTreeMap<String, serde_json::Value>,
}

/// One page of a session timeline.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionEventsPage {
    pub items: Vec<SessionEvent>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ranges: Vec<PayloadRange>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frontier: Vec<SourceFrontier>,
    pub fixed_frontier: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gaps: Vec<SourceGap>,
}

/// The folded state document of a session.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionStateView {
    pub revision: u64,
    pub document: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<StateChange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frontier: Option<SourceFrontier>,
    pub complete: bool,
}

/// One resource a session wrote, recalled, or touched.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionLink {
    pub surface: LinkSurface,
    pub resource: String,
    pub item: String,
    pub relation: LinkRelation,
    pub first: LogPosition,
    pub last: LogPosition,
}

/// The resources linked to one session.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionLinksView {
    pub links: Vec<SessionLink>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frontier: Vec<SourceFrontier>,
    pub truncated: bool,
}

/// The source partitions that hold records of one session.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSources {
    pub sources: Vec<SourceFrontier>,
    /// Registered lane topic ID, creation generation, and partition count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane: Option<(u32, u64, u32)>,
}

/// One committed change batch in a stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionChangeRow {
    pub seq: u64,
    pub sessions: Vec<ConversationId>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub positions: Vec<LogPosition>,
    pub truncated: bool,
}

/// Change rows after a sequence. `resync` is set when the requested sequence
/// is below the retained floor, so the reader must list sessions again.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionChanges {
    pub rows: Vec<SessionChangeRow>,
    pub floor: u64,
    pub resync: bool,
}

/// The reply to every session read: `Ok` with the outcome, or `Err` with a
/// failure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum SessionReply {
    Ok(SessionOutcome),
    Err(SessionError),
}

/// The successful outcome of a session read, shaped per request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum SessionOutcome {
    Info(Box<SessionInfo>),
    Page(SessionPage),
    Events(SessionEventsPage),
    State(SessionStateView),
    Links(SessionLinksView),
    Sources(SessionSources),
    Changes(SessionChanges),
}

/// Why a session read failed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[non_exhaustive]
pub enum SessionError {
    #[error("sessions not supported: {0}")]
    Unsupported(String),
    #[error("session not found: {0}")]
    NotFound(String),
    #[error("stream is not registered for sessions: {0}")]
    NotRegistered(String),
    #[error("invalid session request: {0}")]
    Invalid(String),
    #[error("session read not authorized: {0}")]
    Unauthorized(String),
    /// The request named a stream or topic generation the index does not hold.
    #[error("stale session generation: {0}")]
    Stale(String),
    #[error("session backend error: {0}")]
    Backend(String),
    /// The identical request may succeed later.
    #[error("temporarily unavailable: {0}")]
    Unavailable(String),
}

/// The body of one process heartbeat on `agent.heartbeats`: the sessions in
/// one stream that the process holds leases on. A process with sessions in
/// several streams writes one heartbeat per stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionHeartbeat {
    pub process: String,
    pub stream: String,
    pub sessions: Vec<ConversationId>,
}

impl crate::validate::Validate for SessionHeartbeat {
    fn validate(&self) -> Result<(), crate::error::InvalidError> {
        if self.sessions.len() > crate::limits::MAX_HEARTBEAT_SESSIONS {
            return Err(crate::error::InvalidError::new(
                "heartbeat lists too many sessions",
            ));
        }
        Ok(())
    }
}
