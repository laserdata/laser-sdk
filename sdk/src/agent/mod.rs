mod agdx;
mod assembler;
pub(crate) mod budget;
mod builder;
mod clock;
mod consumer;
pub(crate) mod contract;
pub(crate) mod control;
mod ctx;
mod laser;
pub(crate) mod lease;
mod memory_handler;
pub(crate) mod partitioning;
pub(crate) mod pause;
pub(crate) mod registry;
pub(crate) mod replies;
mod router;
mod scope;
mod session;
mod session_reads;
pub use session_reads::{SessionChange, SessionEventsRequest, SessionListRequest, SessionWatch};
pub(crate) mod session_ops;
pub use session_ops::{
    AssembledContext, ModelCall, ModelRequest, ModelResponse, SessionControl, SessionState,
    SubmitBuilder, Submitted, ToolCall, default_redact,
};
mod state;
mod workflow;

pub use crate::laser::Laser;
pub(crate) use agdx::agdx_headers;
pub use agdx::{
    Agdx, AgdxReceipt, AgdxSend, AgdxStream, DEFAULT_CHUNK_FLUSH_BYTES, DEFAULT_CHUNK_LINGER_MS,
    MAX_CHUNK_BODY_BYTES,
};
pub use assembler::{ChunkAssembler, FINISH_REASON_ABANDONED, FINISH_REASON_GAP, StreamEvent};
pub use builder::{Agent, AgentHandle};
pub use clock::{Clock, SystemClock, TestClock};
pub(crate) use consumer::provenance_and_envelope;
pub use consumer::{
    AgentHandler, AgentMessage, AgentMiddleware, ConcurrencyPolicy, DeadLetterSink, Deduplicator,
    LocalAgentHandler, ReliableConsumer, RetryPolicy, SlidingWindow,
};
pub use contract::{Contract, ContractBuilder, ScatterOutcome, ScatterReport};
pub use control::PendingControl;
pub use ctx::{AgentCtx, Gather, GatherPolicy};
pub use laser::{ConsumerRef, ConsumptionStatus};
pub use lease::SessionLease;
pub use memory_handler::MemoryHandler;
pub use pause::ParkedRecords;
pub use registry::{AgentRegistry, RegisteredCard};
#[cfg(feature = "query")]
pub use registry::{ClientMetadataPage, ClientMetadataRequest};
pub use router::{
    CapabilitySelector, InboxRoute, RouteCandidate, RoutePolicy, RouteScorer, Router,
};
pub use scope::AgentScope;
pub use session::{
    DEFAULT_SESSION_CONTEXT_TOKENS, DEFAULT_SESSION_CONTEXT_TURNS, DEFAULT_SESSION_HEARTBEAT,
    DEFAULT_SESSION_IDLE_TIMEOUT, DEFAULT_SESSION_MEMORY_NAMESPACE, Session, SessionBootstrap,
    SessionBuilder, SessionConfig, SessionLayout, SessionPolicy, SessionTurn, Sessions,
    TopicRetention, derive_session_id,
};
pub use state::{
    ConversationState, ReplayBound, checkpoint_from_snapshot, resume_offsets,
    snapshot_from_checkpoint,
};
#[cfg(feature = "kv")]
pub use workflow::WORKFLOW_FENCE_NAMESPACE;
pub use workflow::{
    OnTimeout, StepContext, StepFn, StepHandle, Verifier, Workflow, WorkflowBudget, WorkflowOutcome,
};
