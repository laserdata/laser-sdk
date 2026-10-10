// The slim prelude: the accessor grammar's entry points and the handful of
// types nearly every program names. One glob, no drowning the IDE. The long
// tail (bridge types, seam traits, projection-control shapes, every memory
// knob) lives in [`full`].
pub use crate::error::LaserError;
pub use crate::types::{AgentId, ConsumerGroupName, ConversationId, MessageId, PrincipalId};

#[cfg(all(feature = "agent", feature = "kv"))]
pub use crate::agent::WORKFLOW_FENCE_NAMESPACE;
#[cfg(feature = "agent")]
pub use crate::agent::{
    Agent, AgentCtx, AgentHandle, AgentHandler, AgentMessage, Contract, ConversationState,
    ReliableConsumer, ReplayBound, RoutePolicy, Router, Session, SessionConfig, SessionLease,
    SessionTurn, Sessions, TopicRetention, Workflow,
};
pub use crate::capabilities::Capabilities;
#[cfg(feature = "agent")]
pub use crate::context::Checkpoint;
#[cfg(feature = "agent")]
pub use crate::context_scope::{ContextScope, ScopedMemory};
#[cfg(feature = "streaming")]
pub use crate::cursor::Cursor;
#[cfg(feature = "destinations")]
pub use crate::destinations::Destinations;
#[cfg(feature = "streaming")]
pub use crate::filters::FilteredReader;
#[cfg(feature = "fork")]
pub use crate::fork::ForkHandle;
#[cfg(feature = "kv")]
pub use crate::kv::{Kv, KvEntry, KvPage};
#[cfg(feature = "streaming")]
pub use crate::laser::{Laser, LaserBuilder, PublishOptions, ResourceNaming};
#[cfg(feature = "agent")]
pub use crate::memory::{Memory, MemoryHandle, MemoryItem};
#[cfg(feature = "streaming")]
pub use crate::message::Message;
#[cfg(feature = "provenance")]
pub use crate::provenance::{AgentTopic, Provenance};
#[cfg(feature = "query")]
pub use crate::query::{QueryResult, Row};
#[cfg(feature = "streaming")]
pub use crate::stream::ContentType;
#[cfg(feature = "streaming")]
pub use crate::stream::{
    CommitPolicy, Consumer, ConsumerGroup, ConsumerMessage, ConsumerStart, GroupFilter, Producer,
    ProducerMessage, Routing, SendMessagesConfirmationResponse, SendMessagesResponse, Stream,
    Topic,
};
#[cfg(feature = "streaming")]
pub use crate::typed::{TypedDecodeError, TypedRecord, TypedRecords, TypedTopic};
#[cfg(feature = "watch")]
pub use crate::watch::{Watch, WatchReader};
#[cfg(feature = "graph")]
pub use laser_wire::graph::EdgeDir;

/// Everything: the slim prelude plus the long tail. For an example, a test,
/// or a file that genuinely touches many surfaces, `use
/// laser_sdk::prelude::full::*;` and stop importing. Application code is
/// usually better served by the slim prelude plus explicit imports.
pub mod full {
    pub use super::*;

    pub use crate::types::IdError;

    #[cfg(feature = "a2a-http")]
    pub use crate::a2a::A2aMethod;
    #[cfg(feature = "a2a-bridge")]
    pub use crate::a2a::{A2aBridge, Artifact, Task, TaskState, TaskStatus};
    #[cfg(feature = "agent")]
    pub use crate::agent::AgentScope;
    #[cfg(feature = "agent")]
    pub use crate::agent::MemoryHandler;
    #[cfg(feature = "agent")]
    pub use crate::agent::{
        Agdx, AgdxSend, AgdxStream, AgentMiddleware, AgentRegistry, CapabilitySelector,
        ChunkAssembler, ConcurrencyPolicy, ContractBuilder, DeadLetterSink, Deduplicator, Gather,
        GatherPolicy, InboxRoute, RegisteredCard, RetryPolicy, RouteCandidate, RouteScorer,
        ScatterOutcome, ScatterReport, SessionPolicy, SlidingWindow, StepContext, StepFn,
        StepHandle, StreamEvent, Verifier, WorkflowBudget, WorkflowOutcome,
    };
    #[cfg(feature = "agui")]
    pub use crate::agui::AgUiEvent;
    #[cfg(feature = "agent")]
    pub use crate::blob::BlobStore;
    pub use crate::capabilities::OpVersions;
    #[cfg(feature = "agent")]
    pub use crate::context::{
        Chain, ContextAssembler, ContextMessage, ContextPolicy, LastN, RoleFilter, TokenBudget,
    };
    #[cfg(feature = "streaming")]
    pub use crate::filters::{
        ConsumerFilter, ExecutionMode, FaultPolicy, FilterBinding, FilterExpr, FilterGroupRef,
        FilteredReaderBuilder, FilteredStart, GroupFilterSpec, MatchedPage, MatchedRecord,
        ReadMode,
    };
    #[cfg(feature = "fork")]
    pub use crate::fork::{ForkInfo, ForkKind, ForkStatus};
    #[cfg(feature = "agent")]
    pub use crate::govern::{
        ActionCounters, ActionDecision, ActionGovernor, ActionKind, GovernedAction, GovernorMode,
        GovernorRetention, PolicyEvidence, PolicyRef, Verdict,
    };
    #[cfg(feature = "graph")]
    pub use crate::graph::GraphHandle;
    #[cfg(feature = "mcp-http")]
    pub use crate::mcp::McpMethod;
    #[cfg(feature = "mcp-bridge")]
    pub use crate::mcp::{
        McpBridge, McpContent, McpPrompt, McpPromptArgument, McpResource, McpTool, McpToolResult,
    };
    #[cfg(feature = "agent")]
    pub use crate::memory::{
        ConsolidationReport, Consolidator, DefaultConsolidator, Embedder, Feedback, Lifetime,
        LogMemory, MemoryBackend, MemoryClass, MemoryId, MemoryKind, MemoryQuery, MemoryScope,
        MemoryTopicBuilder, RecallBuilder, RecallSignal, RecallStrategy, RememberBuilder,
        RerankedMemory, Reranker, VectorMemory, fuse_reciprocal_rank, to_context_block,
    };
    #[cfg(feature = "projections")]
    pub use crate::projections::{
        Bindings, Projections, ProjectionsRequest, RegisterSchemaRequest, Schemas,
    };
    #[cfg(feature = "provenance")]
    pub use crate::provenance::{LlmUsage, ProvenanceError, keys};
    #[cfg(feature = "query")]
    pub use crate::query::{
        AggCall, AggFunc, Aggregate, CmpOp, Dir, EdgeExtract, EntitySchema, FieldType, Filter,
        IndexField, IndexSchema, KeyMatch, NodeExtract, Page, Predicate, Projection,
        ProjectionBinding, ProjectionId, ProjectionInfo, ProjectionKind, Query, QueryError,
        QueryRequest, RawSql, RetentionPolicy, SchemaDef, SchemaInfo, SchemaSource, Select, Sort,
        SourceSelector, Value, VectorQuery, Window,
    };
    #[cfg(all(feature = "agent", feature = "kv"))]
    pub use crate::snapshot::KvSnapshotStore;
    #[cfg(feature = "agent")]
    pub use crate::snapshot::{SnapshotStore, TopicSnapshotStore};
    #[cfg(feature = "streaming")]
    pub use crate::stream::{
        BackgroundConfig, BalancedSharding, BatchPublishRequest, Codec, Decoder, DirectConfig,
        IggyConsumer, IggyConsumerBuilder, IggyProducer, IggyProducerBuilder, OrderedSharding,
        PublishRequest, Record, RecordBuilder, Sharding,
    };
    #[cfg(feature = "streaming")]
    pub use crate::stream::{ConsumerGroupInfo, CreateConsumerGroup};
    #[cfg(feature = "destinations")]
    pub use laser_wire::checkpoint::{
        AttemptColumnMetrics, AttemptObject, CheckpointError, CheckpointMutationResult,
        CheckpointOwnerId, CheckpointOwnerLease, CheckpointReadConsistency,
        CheckpointRequestEnvelope, CheckpointRequestId, CompletedAttempt, CredentialGeneration,
        DestinationBlock, DestinationBlockCode, DestinationCheckpointPage,
        DestinationCheckpointStatus, DestinationCheckpointView, DestinationEffectiveState,
        DestinationListFilter, IcebergCommitRequirement, PartitionCheckpoint,
        PartitionLifecycleChange, PartitionLifecycleState, PreparedAttempt, PreparedAttemptId,
        PreparedAttemptSummary, PreparedTableRequirements, PublicCheckpointMutation,
        QueryRoutePage, RepairAction, RepairRecord, RetentionGap, SourceOffsetRange,
    };
    #[cfg(feature = "destinations")]
    pub use laser_wire::destination::{
        BackendBinding, BackendResourceId, DestinationDesiredState, DestinationErrorPolicy,
        DestinationId, DestinationOperationId, FileFormat, MaterializationDestination,
        NewPartitionPolicy, PartitionStart, PhysicalTable, ProjectionRef, QueryRoute, QueryRouteId,
        QueryRouteTarget, RecreatedPartitionPolicy, StartPolicy, TableFormat,
    };
    #[cfg(feature = "graph")]
    pub use laser_wire::graph::{
        EdgeId, GraphEdge, GraphNode, GraphResult, GraphReturn, NodeId, SourceRef,
    };
    #[cfg(feature = "destinations")]
    pub use laser_wire::schema::{
        BinaryValue, DecimalValue, Digest32, FieldValue, LogicalField, LogicalSchema,
        LogicalSchemaId, LogicalSchemaRef, LogicalType, LogicalTypeKind, MapEntry,
        SchemaFingerprint, TypedValue, UuidValue,
    };
    #[cfg(feature = "destinations")]
    pub use laser_wire::source::{PhysicalClusterIncarnation, SourceIncarnation, SourceScope};
    // `Json` and `Msgpack` are codec marker types, intentionally NOT here
    // because the short names collide too easily with user code
    // (`serde_json::Value::Json`, custom `Json` types, etc.). Import
    // explicitly: `use laser_sdk::stream::{Json, Msgpack};`.
    #[cfg(feature = "agent")]
    pub use crate::state_store::{FileStore, InMemoryStore, StateStore};
    // The session and error vocabulary ordinary session, agent, and governance
    // code names: a session budget, its status, the display type a turn folds
    // to, and the structured error body a failure carries.
    #[cfg(feature = "rbac")]
    pub use crate::rbac::{Action, Effect, Feature, Grant, ResourcePattern, Role, delegated_allow};
    #[cfg(feature = "agent")]
    pub use laser_wire::agent::{AgentErrorBody, AgentErrorCode, Budget, SessionStatus};
    #[cfg(feature = "agent")]
    pub use laser_wire::dispatch::DisplayType;
}

#[cfg(all(test, feature = "agent", feature = "rbac"))]
mod tests {
    #[test]
    fn given_the_full_prelude_when_glob_imported_then_should_name_the_session_and_rbac_vocabulary()
    {
        use super::full::*;
        let names = [
            std::any::type_name::<Budget>(),
            std::any::type_name::<SessionStatus>(),
            std::any::type_name::<DisplayType>(),
            std::any::type_name::<AgentErrorBody>(),
            std::any::type_name::<AgentErrorCode>(),
            std::any::type_name::<Action>(),
            std::any::type_name::<Feature>(),
            std::any::type_name::<Effect>(),
        ];
        assert!(names.iter().all(|name| name.starts_with("laser_wire::")));
        assert!(!delegated_allow(&[], &[], Feature::Kv, Action::Read, None));
    }
}
