#![forbid(unsafe_code)]

pub mod agent;
pub mod arrow;
pub mod authz;
pub mod batch;
pub mod browse;
pub mod change;
pub mod clients;
pub mod codes;
pub mod commands;
pub mod content;
pub mod control;
pub mod destination;
#[cfg(feature = "cbor")]
pub mod dispatch;
pub(crate) mod encoding;
pub mod error;
pub mod filter;
pub mod fork;
pub mod forward;
pub mod graph;
pub mod hashing;
pub mod headers;
pub mod hello;
pub mod http;
pub mod keys;
pub mod kv;
pub mod limits;
pub mod memory;
pub mod mutation;
pub mod query;
pub mod result;
pub mod schema;
pub mod session;
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
#[cfg(feature = "schema-decode")]
pub mod writer_schema;

pub mod prelude {
    pub use crate::agent::{
        AgentDeadLetter, AgentEnvelope, AgentErrorBody, AgentErrorCode, AgentId, AgentKind,
        ChannelId, ConversationId, CorrelationId, DeadLetterReason, IdempotencyKey, LogPosition,
        RecordId, TaskState, TokenUsage,
    };
    pub use crate::arrow::{
        ArrowIpcMessageMetadata, ArrowIpcPolicy, ArrowIpcRejectionCode, ArrowTimestampUnit,
    };
    pub use crate::authz::{
        Action, AuthzError, AuthzReply, Effect, Feature, Grant, ResourceKind, ResourcePattern,
        Role, RoleBinding,
    };
    pub use crate::browse::{BrowseOutcome, BrowseReply, ProjectionInfo, SchemaInfo};
    pub use crate::content::ContentType;
    pub use crate::control::{
        ControlCommand, ControlEnvelope, EdgeExtract, EntitySchema, FieldType, IndexField,
        IndexSchema, NodeExtract, Projection, ProjectionBinding, ProjectionId, ProjectionKind,
        RetentionPolicy, SchemaDef, SchemaSource, SourceSelector,
    };
    pub use crate::destination::{BackendBinding, BackendResourceId};
    pub use crate::error::{DecodeError, InvalidError};
    pub use crate::filter::{
        Coerce, ConsumerFilter, FaultPolicy, FieldPath, FilterBinding, FilterCodec, FilterConsumer,
        FilterError, FilterErrorReason, FilterExpr, FilterGroupRef, FilterMutation,
        FilterMutationOutcome, FilterMutationStatus, FilterRef, FilterSource, FilteredPage,
        FilteredStart, StopReason, TimestampFormat,
    };
    pub use crate::fork::{ForkError, ForkInfo, ForkKind, ForkOutcome, ForkReply, ForkStatus};
    pub use crate::graph::{
        EdgeDir, EdgeId, GraphEdge, GraphError, GraphNeighbors, GraphNode, GraphQuery, GraphReply,
        GraphResult, GraphReturn, GraphStart, GraphUpsert, Hop, NodeId, Path,
    };
    pub use crate::hello::{
        BackendAnnounce, BackendDescriptor, BackendDesiredState, BackendImplementation,
        BackendLimits, BackendMode, BackendObservedState, BackendReadiness, BackendReadinessCode,
        BackendReadinessReason, HelloReply, OpVersions, QueryCapabilities, SchemaCapabilities,
    };
    pub use crate::keys::{KeyKind, KeyRecord};
    pub use crate::kv::{
        KvEntry, KvError, KvMetadata, KvNamespaceInfo, KvOutcome, KvPage, KvReply, MemoryRowScope,
    };
    pub use crate::query::{
        AggCall, AggFunc, Aggregate, CmpOp, Consistency, ConsistencyGate, Dir, Filter, KeyMatch,
        Page, Predicate, Query, QueryContext, QueryEngine, QueryEnvelope, QueryError,
        QueryErrorCode, QueryExecutionId, QueryPageRequest, QueryReply, QueryResult, QueryTarget,
        RawSql, ResolvedQueryTarget, Row, Select, Sort, SqlDialect, Value, VectorQuery, Window,
    };
    pub use crate::result::ResultCode;
    pub use crate::schema::{
        BinaryValue, DecimalValue, Digest32, FieldValue, LogicalField, LogicalSchema,
        LogicalSchemaId, LogicalSchemaRef, LogicalType, LogicalTypeKind, MapEntry,
        SchemaFingerprint, TypedValue, UuidValue,
    };
}
