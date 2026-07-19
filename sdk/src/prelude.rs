// The slim prelude: the accessor grammar's entry points and the handful of
// types nearly every program names. One glob, no drowning the IDE. The long
// tail (bridge types, seam traits, projection-control shapes, every memory
// knob) lives in [`full`].
pub use crate::error::LaserError;
pub use crate::types::{AgentId, ConsumerGroupName, ConversationId, MessageId, PrincipalId};

#[cfg(feature = "agent")]
pub use crate::agent::{
    Agent, AgentCtx, AgentHandle, AgentHandler, AgentMessage, Contract, ConversationState,
    ReliableConsumer, ReplayBound, RoutePolicy, Router, Workflow,
};
pub use crate::capabilities::Capabilities;
#[cfg(feature = "agent")]
pub use crate::context_scope::{ContextScope, ScopedMemory};
#[cfg(feature = "streaming")]
pub use crate::cursor::Cursor;
#[cfg(feature = "fork")]
pub use crate::fork::ForkHandle;
#[cfg(feature = "kv")]
pub use crate::kv::{Kv, KvEntry, KvPage};
#[cfg(feature = "streaming")]
pub use crate::laser::{Laser, LaserBuilder};
#[cfg(feature = "agent")]
pub use crate::memory::{Memory, MemoryHandle, MemoryItem};
#[cfg(feature = "streaming")]
pub use crate::message::Message;
#[cfg(feature = "provenance")]
pub use crate::provenance::{AgentTopic, Provenance};
#[cfg(feature = "query")]
pub use crate::query::{QueryResult, Row};
#[cfg(feature = "runs")]
pub use crate::runs::Runs;
#[cfg(feature = "streaming")]
pub use crate::stream::ContentType;
#[cfg(feature = "streaming")]
pub use crate::stream::{
    CommitPolicy, Consumer, ConsumerMessage, ConsumerStart, Producer, ProducerMessage, Routing,
    Stream, Topic,
};
#[cfg(feature = "streaming")]
pub use crate::typed::{TypedDecodeError, TypedRecord, TypedRecords, TypedTopic};
#[cfg(feature = "watch")]
pub use crate::watch::{Watch, WatchReader};
#[cfg(feature = "runs")]
pub use laser_wire::agent_workflow::AgentRunState;
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
        Agdx, AgdxSend, AgdxStream, AgentMiddleware, AgentRegistry, Budget, CapabilitySelector,
        ChunkAssembler, ConcurrencyPolicy, ContractBuilder, DeadLetterSink, Deduplicator, Gather,
        GatherPolicy, InboxRoute, RegisteredCard, RetryPolicy, RouteCandidate, RouteScorer,
        ScatterOutcome, ScatterReport, SessionPolicy, SlidingWindow, StepContext, StepFn,
        StepHandle, StreamEvent, Verifier, WorkflowOutcome,
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
    #[cfg(feature = "fork")]
    pub use crate::fork::{ForkInfo, ForkKind, ForkStatus};
    #[cfg(feature = "agent")]
    pub use crate::govern::{
        ActionCounters, ActionDecision, ActionGovernor, ActionKind, GovernedAction, GovernorMode,
        PolicyEvidence, PolicyRef, Verdict,
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
        AggCall, AggFunc, Aggregate, CmpOp, Delivery, Dir, EdgeExtract, EntitySchema, FieldType,
        Filter, IndexField, IndexSchema, KeyMatch, NodeExtract, Page, Predicate, Projection,
        ProjectionBinding, ProjectionId, ProjectionInfo, ProjectionKind, Query, QueryError,
        QueryRequest, RawSql, RetentionPolicy, SchemaDef, SchemaInfo, SchemaSource, Select, Sort,
        SourceSelector, Target, TargetRole, Value, VectorQuery, Window,
    };
    #[cfg(feature = "runs")]
    pub use crate::runs::RunListRequest;
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
    #[cfg(feature = "runs")]
    pub use laser_wire::agent_workflow::{AgentRunInfo, RunPage};
    #[cfg(feature = "graph")]
    pub use laser_wire::graph::{
        EdgeId, GraphEdge, GraphNode, GraphResult, GraphReturn, NodeId, SourceRef,
    };
    // `Json` and `Msgpack` are codec marker types, intentionally NOT here
    // because the short names collide too easily with user code
    // (`serde_json::Value::Json`, custom `Json` types, etc.). Import
    // explicitly: `use laser_sdk::stream::{Json, Msgpack};`.
    #[cfg(feature = "agent")]
    pub use crate::state_store::{FileStore, InMemoryStore, StateStore};
}
