#![forbid(unsafe_code)]

pub mod agent;
pub mod browse;
pub mod codes;
pub mod commands;
pub mod content;
pub mod control;
pub(crate) mod encoding;
pub mod error;
pub mod fork;
pub mod forward;
pub mod headers;
pub mod hello;
pub mod http;
pub mod kv;
pub mod limits;
pub mod query;
pub mod result;
pub mod topics;

#[cfg(feature = "codecs")]
pub mod codecs;
#[cfg(feature = "fixtures")]
pub mod fixtures;
#[cfg(feature = "cbor")]
pub mod framing;
#[cfg(feature = "http-client")]
pub mod http_client;

pub mod prelude {
    pub use crate::agent::{
        AgentDeadLetter, AgentEnvelope, AgentErrorBody, AgentErrorCode, AgentId, AgentKind,
        ChannelId, ConversationId, CorrelationId, DeadLetterReason, IdempotencyKey, LogPosition,
        RecordId, TaskState, TokenUsage,
    };
    pub use crate::browse::{BrowseOutcome, BrowseReply, ProjectionInfo, SchemaInfo};
    pub use crate::content::ContentType;
    pub use crate::control::{
        ControlCommand, ControlEnvelope, Delivery, FieldType, IndexField, IndexSchema, Projection,
        ProjectionBinding, ProjectionId, RetentionPolicy, SchemaDef, SchemaSource, SourceSelector,
        Target, TargetRole,
    };
    pub use crate::error::{DecodeError, InvalidError};
    pub use crate::fork::{ForkError, ForkInfo, ForkKind, ForkOutcome, ForkReply, ForkStatus};
    pub use crate::hello::{HelloReply, OpVersions};
    pub use crate::kv::{KvEntry, KvError, KvNamespaceInfo, KvOutcome, KvPage, KvReply};
    pub use crate::query::{
        AggCall, AggFunc, Aggregate, CmpOp, Consistency, Dir, Filter, KeyMatch, Page, Predicate,
        Query, QueryEnvelope, QueryError, QueryReply, QueryResult, RawSql, Row, Select, Sort,
        Value, VectorQuery, Window,
    };
    pub use crate::result::ResultCode;
}
