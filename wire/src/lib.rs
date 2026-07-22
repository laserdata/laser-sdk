#![forbid(unsafe_code)]

pub mod agent;
pub mod agent_workflow;
pub mod authz;
pub mod batch;
pub mod browse;
pub mod change;
pub mod clients;
pub mod codes;
pub mod commands;
pub mod content;
pub mod control;
pub(crate) mod encoding;
pub mod error;
pub mod fork;
pub mod forward;
pub mod graph;
pub mod hashing;
pub mod headers;
pub mod hello;
pub mod http;
pub mod kv;
pub mod limits;
pub mod memory;
pub mod mutation;
pub mod query;
pub mod result;
pub mod snapshot;
pub mod topics;
pub mod topology;
pub mod validate;

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
    pub use crate::authz::{
        Action, AuthzError, AuthzReply, Effect, Feature, Grant, ResourceKind, ResourcePattern,
        Role, RoleBinding,
    };
    pub use crate::browse::{BrowseOutcome, BrowseReply, ProjectionInfo, SchemaInfo};
    pub use crate::content::ContentType;
    pub use crate::control::{
        ControlCommand, ControlEnvelope, Delivery, EdgeExtract, EntitySchema, FieldType,
        IndexField, IndexSchema, NodeExtract, Projection, ProjectionBinding, ProjectionId,
        ProjectionKind, RetentionPolicy, SchemaDef, SchemaSource, SourceSelector, Target,
        TargetRole,
    };
    pub use crate::error::{DecodeError, InvalidError};
    pub use crate::fork::{ForkError, ForkInfo, ForkKind, ForkOutcome, ForkReply, ForkStatus};
    pub use crate::graph::{
        EdgeDir, EdgeId, GraphEdge, GraphError, GraphNeighbors, GraphNode, GraphQuery, GraphReply,
        GraphResult, GraphReturn, GraphStart, GraphUpsert, Hop, NodeId, Path,
    };
    pub use crate::hello::{HelloReply, OpVersions};
    pub use crate::kv::{
        KvEntry, KvError, KvMetadata, KvNamespaceInfo, KvOutcome, KvPage, KvReply, MemoryRowScope,
    };
    pub use crate::query::{
        AggCall, AggFunc, Aggregate, CmpOp, Consistency, Dir, Filter, KeyMatch, Page, Predicate,
        Query, QueryEnvelope, QueryError, QueryReply, QueryResult, RawSql, Row, Select, Sort,
        Value, VectorQuery, Window,
    };
    pub use crate::result::ResultCode;
}
