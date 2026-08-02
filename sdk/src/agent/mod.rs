mod agdx;
mod assembler;
mod builder;
mod clock;
mod consumer;
mod contract;
mod ctx;
mod laser;
mod memory_handler;
pub(crate) mod registry;
pub(crate) mod replies;
mod router;
mod scope;
mod session;
mod state;
mod workflow;

pub use crate::laser::Laser;
pub(crate) use agdx::agdx_headers;
pub use agdx::{
    Agdx, AgdxSend, AgdxStream, DEFAULT_CHUNK_FLUSH_BYTES, DEFAULT_CHUNK_LINGER_MS,
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
pub use ctx::{AgentCtx, Gather, GatherPolicy};
pub use laser::{ConsumerRef, ConsumptionStatus};
pub use memory_handler::MemoryHandler;
pub use registry::{AgentRegistry, RegisteredCard};
#[cfg(feature = "query")]
pub use registry::{ClientMetadataPage, ClientMetadataRequest};
pub use router::{
    CapabilitySelector, InboxRoute, RouteCandidate, RoutePolicy, RouteScorer, Router,
};
pub use scope::AgentScope;
pub use session::SessionPolicy;
pub use state::{ConversationState, ReplayBound, resume_offsets};
pub use workflow::{
    Budget, OnTimeout, StepContext, StepFn, StepHandle, Verifier, Workflow, WorkflowOutcome,
};
