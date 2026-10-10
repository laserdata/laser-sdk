use crate::agent::{
    AgentEnvelope, AgentId, AgentKind, METADATA_ROLE, OPERATION_CHAT, OPERATION_PROGRESS,
    OPERATION_REASONING, OPERATION_SESSION, OPERATION_SESSION_PARKED, OPERATION_SESSION_UNPARKED,
    OPERATION_STATE_DELTA, OPERATION_STATE_SNAPSHOT, OPERATION_TASK, TaskState,
};
use crate::filter::{ConsumerFilter, FilterExpr};
use crate::headers::TARGET_AGENT_ID;
use crate::memory::MemoryRecord;
use crate::query::{CmpOp, Value};
use crate::schema::TypedValue;
use crate::topics::{AGENT_CONTROL, AGENT_HEARTBEATS};

pub use crate::headers::BROADCAST;

/// The `command` operation that asks a session to pause.
pub const OPERATION_SESSION_PAUSE: &str = "session_pause";
/// The `command` operation that asks a paused session to resume.
pub const OPERATION_SESSION_RESUME: &str = "session_resume";
/// The `command` operation that asks a session to cancel.
pub const OPERATION_SESSION_CANCEL: &str = "session_cancel";
/// The `command` operation that ends a session as canceled without its agent.
pub const OPERATION_FORCE_CANCEL: &str = "force_cancel";
/// The OTel operation for a text completion model call.
pub const OPERATION_TEXT_COMPLETION: &str = "text_completion";
/// The OTel operation for a content generation model call.
pub const OPERATION_GENERATE_CONTENT: &str = "generate_content";
/// The OTel operation for a tool call.
pub const OPERATION_EXECUTE_TOOL: &str = "execute_tool";
/// The OTel operation for a call to another agent.
pub const OPERATION_INVOKE_AGENT: &str = "invoke_agent";
/// The `event` operation that records an assembled model context.
pub const OPERATION_CONTEXT_ASSEMBLED: &str = "context_assembled";
/// The `event` operation that records a context compaction.
pub const OPERATION_CONTEXT_COMPACTED: &str = "context_compacted";
/// The `event` operation that records a context retrieval.
pub const OPERATION_CONTEXT_RETRIEVED: &str = "context_retrieved";
/// The `event` operation that records a policy decision.
pub const OPERATION_POLICY_DECISION: &str = "policy_decision";

/// The session control operations. They are honored only on `agent.control`.
pub const CONTROL_OPERATIONS: [&str; 4] = [
    OPERATION_SESSION_PAUSE,
    OPERATION_SESSION_RESUME,
    OPERATION_SESSION_CANCEL,
    OPERATION_FORCE_CANCEL,
];

/// One record a session timeline can show.
#[derive(Clone, Copy, Debug)]
pub enum SessionRecord<'a> {
    Envelope(&'a AgentEnvelope),
    Memory(&'a MemoryRecord),
    DeadLetter,
    Journal,
    KvMutation,
    GraphMutation,
    /// A record whose body does not decode, with its kind when the header
    /// names one.
    Undecodable(Option<AgentKind>),
}

/// How a session timeline shows one record. The value is derived from the
/// record and never stored as a separate field.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, strum::IntoStaticStr, strum::EnumString)]
pub enum DisplayType {
    #[strum(serialize = "session.submitted")]
    SessionSubmitted,
    #[strum(serialize = "session.started")]
    SessionStarted,
    #[strum(serialize = "session.paused")]
    SessionPaused,
    #[strum(serialize = "session.resumed")]
    SessionResumed,
    #[strum(serialize = "session.completed")]
    SessionCompleted,
    #[strum(serialize = "session.failed")]
    SessionFailed,
    #[strum(serialize = "session.canceled")]
    SessionCanceled,
    #[strum(serialize = "session.heartbeat")]
    SessionHeartbeat,
    /// A work record a paused session's agent held.
    #[strum(serialize = "session.parked")]
    SessionParked,
    /// A held work record handled after the session resumed.
    #[strum(serialize = "session.unparked")]
    SessionUnparked,
    #[strum(serialize = "session.control")]
    SessionControl,
    #[strum(serialize = "user.message")]
    UserMessage,
    #[strum(serialize = "model.request")]
    ModelRequest,
    #[strum(serialize = "model.response")]
    ModelResponse,
    #[strum(serialize = "model.stream")]
    ModelStream,
    #[strum(serialize = "tool.call")]
    ToolCall,
    #[strum(serialize = "tool.result")]
    ToolResult,
    #[strum(serialize = "agent.handoff")]
    AgentHandoff,
    /// An agent record that matches no more specific row.
    #[strum(serialize = "agent.message")]
    AgentMessage,
    #[strum(serialize = "state.updated")]
    StateUpdated,
    #[strum(serialize = "context.assembled")]
    ContextAssembled,
    #[strum(serialize = "context.compacted")]
    ContextCompacted,
    #[strum(serialize = "context.retrieved")]
    ContextRetrieved,
    #[strum(serialize = "memory.created")]
    MemoryCreated,
    #[strum(serialize = "memory.forgotten")]
    MemoryForgotten,
    #[strum(serialize = "memory.feedback")]
    MemoryFeedback,
    #[strum(serialize = "task.status")]
    TaskStatus,
    #[strum(serialize = "workflow.step")]
    WorkflowStep,
    #[strum(serialize = "policy.decision")]
    PolicyDecision,
    #[strum(serialize = "error")]
    Error,
    #[strum(serialize = "dead_letter")]
    DeadLetter,
    #[strum(serialize = "undecodable")]
    Undecodable,
    /// A record whose header and body name different identities.
    #[strum(serialize = "invalid")]
    Invalid,
    #[strum(serialize = "kv.set")]
    KvSet,
    #[strum(serialize = "graph.upsert")]
    GraphUpsert,
    #[strum(serialize = "unauthorized_control")]
    UnauthorizedControl,
}

impl DisplayType {
    /// The pinned display word.
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

impl std::fmt::Display for DisplayType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// How a session timeline shows `record`, read from the topic named `topic`.
pub fn display_type(record: SessionRecord<'_>, topic: &str) -> DisplayType {
    let envelope = match record {
        SessionRecord::Envelope(envelope) => envelope,
        SessionRecord::Memory(MemoryRecord::Item { .. }) => return DisplayType::MemoryCreated,
        SessionRecord::Memory(MemoryRecord::Forget { .. }) => return DisplayType::MemoryForgotten,
        SessionRecord::Memory(MemoryRecord::Feedback { .. }) => {
            return DisplayType::MemoryFeedback;
        }
        SessionRecord::DeadLetter => return DisplayType::DeadLetter,
        SessionRecord::Journal => return DisplayType::WorkflowStep,
        SessionRecord::KvMutation => return DisplayType::KvSet,
        SessionRecord::GraphMutation => return DisplayType::GraphUpsert,
        SessionRecord::Undecodable(_) => return DisplayType::Undecodable,
    };
    let operation = envelope.operation.as_deref().unwrap_or_default();
    match envelope.kind {
        AgentKind::Status => status_display(envelope, operation, topic),
        AgentKind::Command if is_control(operation) => {
            if topic == AGENT_CONTROL {
                DisplayType::SessionControl
            } else {
                DisplayType::UnauthorizedControl
            }
        }
        AgentKind::Command if is_user(envelope) => DisplayType::UserMessage,
        AgentKind::Command if is_model(operation) => DisplayType::ModelRequest,
        AgentKind::Command if operation == OPERATION_EXECUTE_TOOL => DisplayType::ToolCall,
        AgentKind::Command if operation == OPERATION_INVOKE_AGENT => DisplayType::AgentHandoff,
        AgentKind::Event if is_user(envelope) => DisplayType::UserMessage,
        AgentKind::Event => match operation {
            OPERATION_STATE_DELTA | OPERATION_STATE_SNAPSHOT => DisplayType::StateUpdated,
            OPERATION_CONTEXT_ASSEMBLED => DisplayType::ContextAssembled,
            OPERATION_CONTEXT_COMPACTED => DisplayType::ContextCompacted,
            OPERATION_CONTEXT_RETRIEVED => DisplayType::ContextRetrieved,
            OPERATION_POLICY_DECISION => DisplayType::PolicyDecision,
            OPERATION_SESSION_PARKED => DisplayType::SessionParked,
            OPERATION_SESSION_UNPARKED => DisplayType::SessionUnparked,
            _ => DisplayType::AgentMessage,
        },
        AgentKind::Response if is_model(operation) => DisplayType::ModelResponse,
        AgentKind::Response | AgentKind::Error if operation == OPERATION_EXECUTE_TOOL => {
            DisplayType::ToolResult
        }
        AgentKind::Error => DisplayType::Error,
        AgentKind::Chunk if operation == OPERATION_CHAT || operation == OPERATION_REASONING => {
            DisplayType::ModelStream
        }
        _ => DisplayType::AgentMessage,
    }
}

fn status_display(envelope: &AgentEnvelope, operation: &str, topic: &str) -> DisplayType {
    if operation == OPERATION_PROGRESS && topic == AGENT_HEARTBEATS {
        return DisplayType::SessionHeartbeat;
    }
    if operation != OPERATION_SESSION {
        return if operation == OPERATION_TASK {
            DisplayType::TaskStatus
        } else {
            DisplayType::AgentMessage
        };
    }
    match envelope.task_state {
        Some(TaskState::Submitted) => DisplayType::SessionSubmitted,
        Some(TaskState::Working) if is_session_start(&envelope.body) => DisplayType::SessionStarted,
        Some(TaskState::Working) => DisplayType::SessionResumed,
        Some(TaskState::Paused) => DisplayType::SessionPaused,
        Some(TaskState::Completed) => DisplayType::SessionCompleted,
        Some(TaskState::Failed | TaskState::Rejected) => DisplayType::SessionFailed,
        Some(TaskState::Canceled) => DisplayType::SessionCanceled,
        _ => DisplayType::TaskStatus,
    }
}

// A start body names the agent and SDK. A transition body names neither.
fn is_session_start(body: &[u8]) -> bool {
    crate::framing::decode_named::<ciborium::value::Value>(body).is_ok_and(|value| {
        matches!(value, ciborium::value::Value::Map(entries)
        if entries.iter().any(|(key, _)| {
            matches!(key, ciborium::value::Value::Text(name) if name == "agent" || name == "sdk")
        }))
    })
}

fn is_control(operation: &str) -> bool {
    CONTROL_OPERATIONS.contains(&operation)
}

fn is_model(operation: &str) -> bool {
    matches!(
        operation,
        OPERATION_CHAT | OPERATION_TEXT_COMPLETION | OPERATION_GENERATE_CONTENT
    )
}

fn is_user(envelope: &AgentEnvelope) -> bool {
    envelope
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get(METADATA_ROLE))
        .is_some_and(|role| matches!(role, Value::Str(role) if role == "user"))
}

/// What a reliable consumer does with one record it read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, strum::IntoStaticStr, strum::EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum Dispatch {
    /// A command addressed to this agent for an operation its handler serves.
    Work,
    /// A record that informs the timeline and is never handled: events, model
    /// and tool records for operations the handler does not serve, and control
    /// operations found outside `agent.control`.
    Observational,
    /// A response, error, or chunk. Reply waiters read it, handlers never do.
    Reply,
    /// A status record.
    Lifecycle,
    /// A control operation on `agent.control`, for the control follower.
    Control,
    /// A record addressed to another agent.
    Foreign,
}

impl Dispatch {
    /// The pinned snake-case word.
    pub fn as_str(self) -> &'static str {
        self.into()
    }
}

/// The command operations a handler serves.
#[derive(Clone, Copy, Debug)]
pub enum HandledOperations<'a> {
    /// Every command operation.
    Any,
    /// Only the listed operations.
    Only(&'a [&'a str]),
}

impl HandledOperations<'_> {
    fn handles(self, operation: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Only(operations) => operations.contains(&operation),
        }
    }
}

/// What the agent `me` does with `envelope`, read from the topic named
/// `topic`. The author is never a discriminator, so an agent may send work to
/// itself.
pub fn classify(
    envelope: &AgentEnvelope,
    topic: &str,
    me: &AgentId,
    handled: HandledOperations<'_>,
) -> Dispatch {
    let operation = envelope.operation.as_deref().unwrap_or_default();
    match envelope.kind {
        AgentKind::Status => Dispatch::Lifecycle,
        AgentKind::Response | AgentKind::Error | AgentKind::Chunk => Dispatch::Reply,
        AgentKind::Event => Dispatch::Observational,
        AgentKind::Command => {
            if envelope.target.as_ref().is_some_and(|target| target != me) {
                return Dispatch::Foreign;
            }
            match (is_control(operation), topic == AGENT_CONTROL) {
                (true, true) => Dispatch::Control,
                (true, false) => Dispatch::Observational,
                (false, true) => Dispatch::Foreign,
                (false, false) if handled.handles(operation) => Dispatch::Work,
                (false, false) => Dispatch::Observational,
            }
        }
    }
}

/// The headers-only group filter that selects records addressed to `me` or to
/// every agent. Its digest differs per agent identity.
pub fn addressee_filter(me: &AgentId) -> ConsumerFilter {
    ConsumerFilter::headers_only(FilterExpr::header(
        TARGET_AGENT_ID,
        CmpOp::In,
        TypedValue::List(vec![
            TypedValue::String(me.as_str().to_owned()),
            TypedValue::String(BROADCAST.to_owned()),
        ]),
    ))
}

/// The headers-only group filter that selects broadcast records only.
pub fn broadcast_filter() -> ConsumerFilter {
    ConsumerFilter::headers_only(FilterExpr::header(
        TARGET_AGENT_ID,
        CmpOp::Eq,
        TypedValue::String(BROADCAST.to_owned()),
    ))
}

/// What the agent `me` does with a generic record, one without an envelope,
/// read from the topic named `topic`. Such a record has no kind, so a record
/// that names both its causal parent and a correlation is a reply, and any
/// other record is work unless it is addressed to another agent.
pub fn classify_generic(
    addressee: Option<&str>,
    has_cause: bool,
    has_correlation: bool,
    topic: &str,
    me: &AgentId,
) -> Dispatch {
    if addressee.is_some_and(|to| to != me.as_str() && to != BROADCAST) || topic == AGENT_CONTROL {
        return Dispatch::Foreign;
    }
    if has_cause && has_correlation {
        Dispatch::Reply
    } else {
        Dispatch::Work
    }
}
