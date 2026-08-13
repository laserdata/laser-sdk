#![forbid(unsafe_code)]

pub mod agent;
pub mod agent_workflow;
pub mod arrow;
pub mod authz;
pub mod batch;
pub mod browse;
pub mod change;
pub mod checkpoint;
pub mod clients;
pub mod codes;
pub mod commands;
pub mod content;
pub mod control;
pub mod destination;
pub(crate) mod encoding;
pub mod error;
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
pub mod snapshot;
pub mod source;
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
    pub use crate::arrow::{
        ArrowIpcMessageMetadata, ArrowIpcPolicy, ArrowIpcRejectionCode, ArrowTimestampUnit,
    };
    pub use crate::authz::{
        Action, AuthzError, AuthzReply, Effect, Feature, Grant, ResourceKind, ResourcePattern,
        Role, RoleBinding, SupervisorActorAssertion, SupervisorActorClaims,
        SupervisorAssertionAction,
    };
    pub use crate::browse::{BrowseOutcome, BrowseReply, ProjectionInfo, SchemaInfo};
    pub use crate::checkpoint::{
        AttemptColumnMetrics, AttemptObject, CheckpointError, CheckpointMutationResult,
        CheckpointMutationStamp, CheckpointOwnerId, CheckpointOwnerLease,
        CheckpointReadConsistency, CheckpointReply, CheckpointRequestEnvelope, CheckpointRequestId,
        CompletedAttempt, CredentialGeneration, DestinationBlock, DestinationBlockCode,
        DestinationCheckpointPage, DestinationCheckpointStatus, DestinationCheckpointView,
        DestinationEffectiveState, DestinationGetRequest, DestinationListFilter,
        DestinationListRequest, IcebergCommitRequirement, PartitionCheckpoint,
        PartitionLifecycleChange, PartitionLifecycleState, PreparedAttempt, PreparedAttemptId,
        PreparedAttemptSummary, PreparedTableRequirements, PublicCheckpointMutation,
        QueryRouteListRequest, QueryRoutePage, RepairAction, RepairRecord,
        ReplicatedCheckpointMutation, ReplicatedCheckpointMutationBody, RetentionGap,
        SourceOffsetRange,
    };
    pub use crate::content::ContentType;
    pub use crate::control::{
        ControlCommand, ControlEnvelope, EdgeExtract, EntitySchema, FieldType, IndexField,
        IndexSchema, NodeExtract, Projection, ProjectionBinding, ProjectionId, ProjectionKind,
        RetentionPolicy, SchemaDef, SchemaSource, SourceSelector,
    };
    pub use crate::destination::{
        BackendBinding, BackendResourceId, DestinationDesiredState, DestinationErrorPolicy,
        DestinationId, DestinationOperationId, FileFormat, MaterializationDestination,
        NewPartitionPolicy, PartitionStart, PhysicalTable, ProjectionRef, QueryRoute, QueryRouteId,
        QueryRouteTarget, RecreatedPartitionPolicy, StartPolicy, TableFormat,
    };
    pub use crate::error::{DecodeError, InvalidError};
    pub use crate::fork::{ForkError, ForkInfo, ForkKind, ForkOutcome, ForkReply, ForkStatus};
    pub use crate::graph::{
        EdgeDir, EdgeId, GraphEdge, GraphError, GraphNeighbors, GraphNode, GraphQuery, GraphReply,
        GraphResult, GraphReturn, GraphStart, GraphUpsert, Hop, NodeId, Path,
    };
    pub use crate::hello::{
        BackendAnnounce, BackendDescriptor, BackendDesiredState, BackendImplementation,
        BackendLimits, BackendMode, BackendObservedState, BackendReadiness, BackendReadinessCode,
        BackendReadinessReason, HelloReply, MaintenanceCapabilities, MaterializationCapability,
        OpVersions, QueryCapabilities, QueryPagingCapability, SchemaCapabilities,
        TimeTravelCapability,
    };
    pub use crate::keys::{KeyKind, KeyRecord};
    pub use crate::kv::{
        KvEntry, KvError, KvMetadata, KvNamespaceInfo, KvOutcome, KvPage, KvReply, MemoryRowScope,
    };
    pub use crate::query::{
        AggCall, AggFunc, Aggregate, BoundaryRelation, CmpOp, Consistency, ConsistencyGate, Dir,
        Filter, KeyMatch, MaterializationBoundary, Page, Predicate, Query, QueryCancelEnvelope,
        QueryCancelReply, QueryContext, QueryEngine, QueryEnvelope, QueryError, QueryErrorCode,
        QueryExecutionId, QueryExecutionState, QueryExecutionStatus, QueryPageEnvelope,
        QueryPageRequest, QueryReply, QueryResult, QueryStatusEnvelope, QueryStatusReply,
        QueryTarget, RawSql, ResolvedQueryTarget, Row, Select, SnapshotSelector, Sort, SqlDialect,
        Value, VectorQuery, Window,
    };
    pub use crate::result::ResultCode;
    pub use crate::schema::{
        BinaryValue, DecimalValue, Digest32, FieldValue, LogicalField, LogicalSchema,
        LogicalSchemaId, LogicalSchemaRef, LogicalType, LogicalTypeKind, MapEntry,
        SchemaFingerprint, TypedValue, UuidValue,
    };
    pub use crate::source::{
        PhysicalClusterIncarnation, SourceCut, SourceIncarnation, SourcePartitionCut, SourceScope,
    };
}
