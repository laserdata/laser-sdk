use crate::agent::{
    AgentId, Budget, ConversationId, CorrelationId, LogPosition, SdkInfo, SessionStatus, TokenUsage,
};
use crate::error::InvalidError;
use crate::graph::SourceRef;
use crate::limits::{
    MAX_FILTER_SOURCE_NAME_BYTES, MAX_HEARTBEAT_SESSIONS, MAX_SESSION_LABEL_BYTES,
    MAX_SESSION_TEXT_QUERY_BYTES,
};
use crate::schema::Digest32;
use crate::validate::Validate;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// The resource surface used to narrow a session link read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    pub struct SessionGet {
        pub stream: String,
        pub id: ConversationId,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
        /// The 1-based page. Zero reads the first page.
        pub page: u32,
        /// The sessions per page. Zero leaves it to the server.
        pub results: u32,
        /// The bound of a walk: the `as_of` its first page returned. Later
        /// pages pass it back so sessions folded after it stay out.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub as_of: Option<u64>,
        /// The change series `as_of` belongs to, from the same first page.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub series: Option<u64>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    pub struct SessionEvents {
        pub stream: String,
        pub id: ConversationId,
        /// The 1-based page. Zero reads the first page.
        pub page: u32,
        /// The events per page. Zero leaves it to the server.
        pub results: u32,
        /// The bound of a walk: the `as_of` its first page returned. Later
        /// pages pass it back so events folded after it stay out.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub as_of: Option<u64>,
        /// The change series `as_of` belongs to, from the same first page.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub series: Option<u64>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    pub struct SessionState {
        pub stream: String,
        pub id: ConversationId,
        /// The most history rows. Zero leaves it to the server.
        pub history_limit: u32,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    pub struct SessionLinks {
        pub stream: String,
        pub id: ConversationId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub surface: Option<LinkSurface>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    pub struct SessionSources {
        pub stream: String,
        pub id: ConversationId,
        /// Return only the registered lane identity for a native lane writer.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        pub lane_only: bool,
    }

    /// A page of the change feed. Page `p` of size `r` holds the changes with
    /// sequence `(p - 1) * r + 1 ..= p * r`, so appends and pruning never
    /// shift a page.
    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
    pub struct SessionChanges {
        pub stream: String,
        /// The 1-based page. Zero reads the first page.
        pub page: u32,
        /// The changes per page. Zero leaves it to the server.
        pub results: u32,
    }
}

/// How far the session index has folded one source partition. Offsets are
/// never compared across topics. `topic_generation` is the topic creation time
/// in microseconds, so a recreated topic with a reused id is a different source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct PayloadRange {
    pub topic_id: u32,
    pub topic_generation: u64,
    pub partition_id: u32,
    pub first: u64,
    pub last: u64,
}

/// What the fold did with one state record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum StateOutcome {
    /// The patch or snapshot changed the document.
    Applied,
    /// A patch operation failed, so the whole patch was rejected.
    Rejected,
    /// The base revision did not match the applied revision, the base operation id did not match
    /// the delta that produced it, or a snapshot at it held another document.
    Stale,
    /// The operation id was already applied.
    Duplicate,
    #[serde(other)]
    Unrecognized,
}

/// One entry in the state history of a session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SessionFlags {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub label_truncated: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub overflow: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub events_truncated: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub lane_conflict: bool,
    /// A control record arrived on a partition other than the session's
    /// control route and was not applied.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub control_conflict: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub rebuilding: bool,
    /// Liveness is not known yet because the heartbeat tail has not caught
    /// up, so `idle` is not meaningful.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub liveness_unknown: bool,
}

/// The summary of one session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
    /// The participants of the pause in force that have not acknowledged it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub awaiting: Vec<AgentId>,
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

/// One page of session summaries, newest created first. `page` counts from 1.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SessionPage {
    pub items: Vec<SessionInfo>,
    pub page: u32,
    pub total_pages: u32,
    pub total_results: u32,
    /// The newest change the page covers. Pass it back with `series` for the
    /// next page of the same walk.
    pub as_of: u64,
    /// The change series of the serving index.
    pub series: u64,
    /// The rows a substring search examined.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub searched: Option<u64>,
    /// A substring search filled its scan window, so `total_results` counts
    /// only the matches inside it.
    pub truncated: bool,
    /// The index is rebuilding on this node, so `items` and `total` can be incomplete.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub rebuilding: bool,
}

/// One indexed record on a session timeline. The summary never holds prompt
/// text or tool argument values.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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

/// One page of a session timeline in server append time order. `page` counts
/// from 1.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SessionEventsPage {
    pub items: Vec<SessionEvent>,
    pub page: u32,
    pub total_pages: u32,
    pub total_results: u32,
    /// The newest change the page covers. Pass it back with `series` for the
    /// next page of the same walk.
    pub as_of: u64,
    /// The change series of the serving index.
    pub series: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ranges: Vec<PayloadRange>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frontier: Vec<SourceFrontier>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gaps: Vec<SourceGap>,
}

/// The folded state document of a session.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SessionStateView {
    pub revision: u64,
    pub document: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub history: Vec<StateChange>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frontier: Option<SourceFrontier>,
    pub complete: bool,
    /// The `op_id` of the delta that produced `revision`, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head_op_id: Option<String>,
}

/// One resource a session wrote, recalled, or touched.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SessionLinksView {
    pub links: Vec<SessionLink>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frontier: Vec<SourceFrontier>,
    pub truncated: bool,
}

/// The source partitions that hold records of one session.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SessionSources {
    pub sources: Vec<SourceFrontier>,
    /// Registered lane topic ID, creation generation, and partition count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane: Option<(u32, u64, u32)>,
}

/// One committed change batch in a stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SessionChangeRow {
    pub seq: u64,
    pub sessions: Vec<ConversationId>,
    /// The records behind the change: the folded records for `records`, the
    /// link source records for `links`, and for `expiry` the folded record
    /// whose time expired the sessions. Empty for `maintenance` and for an
    /// expiry the clock decided.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub positions: Vec<LogPosition>,
    pub truncated: bool,
    #[serde(default, skip_serializing_if = "SessionChangeOrigin::is_records")]
    pub origin: SessionChangeOrigin,
}

/// What made the sessions of one change row change.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SessionChangeOrigin {
    /// Records folded from the session sources.
    #[default]
    Records,
    /// Links written by managed mutations or projections.
    Links,
    /// A source partition was reset or left the registration, and its
    /// sessions were rebuilt.
    Maintenance,
    /// Sessions idle past their retention were removed.
    Expiry,
}

impl SessionChangeOrigin {
    pub fn is_records(&self) -> bool {
        *self == Self::Records
    }
}

/// One page of the change feed in sequence order. `total_results` is the
/// newest sequence, pruned changes included, and `floor` the oldest retained
/// sequence, or the next one when none is retained. A follower whose `series`
/// differs, or whose next sequence is below `floor`, lists sessions again.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SessionChanges {
    pub items: Vec<SessionChangeRow>,
    pub page: u32,
    pub total_pages: u32,
    pub total_results: u64,
    /// The page size the server used, which fixes the sequence range of a page.
    pub results: u32,
    pub floor: u64,
    /// The registration's change series.
    pub series: u64,
}

/// The reply to every session read: `Ok` with the outcome, or `Err` with a
/// failure.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub enum SessionReply {
    Ok(SessionOutcome),
    Err(SessionError),
}

/// The successful outcome of a session read, shaped per request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
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
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SessionHeartbeat {
    pub process: String,
    pub stream: String,
    pub sessions: Vec<ConversationId>,
    /// The creation time, in epoch microseconds, of the stream the leases
    /// were taken on. `None` when the writer does not know it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_generation: Option<u64>,
}

impl SessionHeartbeat {
    /// Whether a reader counts this heartbeat for the stream it read it from,
    /// created at `stream_generation`. A stream deleted and created again
    /// keeps its name, so a heartbeat that names another generation is ignored.
    pub fn counts_for(&self, stream_generation: u64) -> bool {
        self.stream_generation
            .is_none_or(|generation| generation == stream_generation)
    }
}

impl Validate for request::SessionList {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.stream.is_empty() || self.stream.len() > MAX_FILTER_SOURCE_NAME_BYTES {
            return Err(InvalidError::new(format!(
                "session list stream must be 1..={MAX_FILTER_SOURCE_NAME_BYTES} bytes"
            )));
        }
        let bounded = [
            ("label prefix", &self.label_prefix, MAX_SESSION_LABEL_BYTES),
            ("text", &self.text, MAX_SESSION_TEXT_QUERY_BYTES),
        ];
        for (label, value, cap) in bounded {
            if let Some(value) = value
                && value.len() > cap
            {
                return Err(InvalidError::new(format!(
                    "session list {label} is {}B, exceeds cap {cap}B",
                    value.len()
                )));
            }
        }
        if self.as_of.is_some() != self.series.is_some() {
            return Err(InvalidError::new(
                "session list as_of and series name one bound, set both or neither",
            ));
        }
        Ok(())
    }
}

impl Validate for SessionHeartbeat {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.sessions.len() > MAX_HEARTBEAT_SESSIONS {
            return Err(InvalidError::new("heartbeat lists too many sessions"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_session_list_over_its_caps_when_validated_then_should_refuse_it() {
        let list = request::SessionList {
            stream: "agents".to_owned(),
            status: None,
            root: None,
            label_prefix: None,
            agent: None,
            text: None,
            page: 0,
            results: 0,
            as_of: None,
            series: None,
        };
        list.validate().expect("a plain session list is valid");
        request::SessionList {
            as_of: Some(4405),
            series: Some(77),
            ..list.clone()
        }
        .validate()
        .expect("a bound names its sequence and series");
        request::SessionList {
            stream: "s".repeat(MAX_FILTER_SOURCE_NAME_BYTES),
            label_prefix: Some("l".repeat(MAX_SESSION_LABEL_BYTES)),
            text: Some("t".repeat(MAX_SESSION_TEXT_QUERY_BYTES)),
            ..list.clone()
        }
        .validate()
        .expect("every filter at its cap is valid");
        request::SessionList {
            text: Some(String::new()),
            ..list.clone()
        }
        .validate()
        .expect("an empty text is within its cap");
        for invalid in [
            request::SessionList {
                stream: String::new(),
                ..list.clone()
            },
            request::SessionList {
                stream: "s".repeat(MAX_FILTER_SOURCE_NAME_BYTES + 1),
                ..list.clone()
            },
            request::SessionList {
                label_prefix: Some("l".repeat(MAX_SESSION_LABEL_BYTES + 1)),
                ..list.clone()
            },
            request::SessionList {
                text: Some("t".repeat(MAX_SESSION_TEXT_QUERY_BYTES + 1)),
                ..list.clone()
            },
            request::SessionList {
                as_of: Some(4405),
                ..list.clone()
            },
            request::SessionList {
                series: Some(77),
                ..list.clone()
            },
        ] {
            assert!(invalid.validate().is_err(), "{invalid:?}");
        }
    }

    #[test]
    fn given_a_heartbeat_when_checked_against_a_stream_generation_then_should_count_unless_it_names_another()
     {
        let heartbeat = |stream_generation| SessionHeartbeat {
            process: "worker".to_owned(),
            stream: "agents".to_owned(),
            sessions: Vec::new(),
            stream_generation,
        };
        assert!(
            heartbeat(None).counts_for(7),
            "a heartbeat that names no generation counts"
        );
        assert!(heartbeat(Some(7)).counts_for(7));
        assert!(
            !heartbeat(Some(6)).counts_for(7),
            "a heartbeat of another stream generation is ignored"
        );
    }
}
