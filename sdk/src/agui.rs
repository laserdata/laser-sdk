use crate::context::ContextAssembler;
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::ConversationId;
use laser_wire::agent::{
    AgentEnvelope, AgentId, AgentKind, OPERATION_REASONING, OPERATION_STATE_DELTA,
    OPERATION_STATE_SNAPSHOT, OPERATION_TASK, OPERATION_TOOL_ARGS, TaskState,
};
use laser_wire::content::ContentType;
use serde::Serialize;
use serde_json::Value as JsonValue;

/// An AG-UI protocol event, tagged by its SCREAMING_SNAKE `type` on the wire.
/// The events AGDX renders directly from the log: chat chunk streams become text
/// messages, reasoning chunk streams become reasoning messages, `tool_args`
/// chunk streams become tool calls (with the answering envelope as the tool
/// result), `status` task updates become run lifecycle, state events become
/// state events, and an error terminal becomes a run error.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum AgUiEvent {
    #[serde(rename = "RUN_STARTED")]
    RunStarted {
        #[serde(rename = "threadId")]
        thread_id: String,
        #[serde(rename = "runId")]
        run_id: String,
    },
    #[serde(rename = "RUN_FINISHED")]
    RunFinished {
        #[serde(rename = "threadId")]
        thread_id: String,
        #[serde(rename = "runId")]
        run_id: String,
    },
    #[serde(rename = "TEXT_MESSAGE_START")]
    TextMessageStart {
        #[serde(rename = "messageId")]
        message_id: String,
        role: String,
    },
    #[serde(rename = "TEXT_MESSAGE_CONTENT")]
    TextMessageContent {
        #[serde(rename = "messageId")]
        message_id: String,
        delta: String,
    },
    #[serde(rename = "TEXT_MESSAGE_END")]
    TextMessageEnd {
        #[serde(rename = "messageId")]
        message_id: String,
    },
    #[serde(rename = "REASONING_MESSAGE_START")]
    ReasoningMessageStart {
        #[serde(rename = "messageId")]
        message_id: String,
        role: String,
    },
    #[serde(rename = "REASONING_MESSAGE_CONTENT")]
    ReasoningMessageContent {
        #[serde(rename = "messageId")]
        message_id: String,
        delta: String,
    },
    #[serde(rename = "REASONING_MESSAGE_END")]
    ReasoningMessageEnd {
        #[serde(rename = "messageId")]
        message_id: String,
    },
    #[serde(rename = "TOOL_CALL_START")]
    ToolCallStart {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        #[serde(rename = "toolCallName")]
        tool_call_name: String,
    },
    #[serde(rename = "TOOL_CALL_ARGS")]
    ToolCallArgs {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        delta: String,
    },
    #[serde(rename = "TOOL_CALL_END")]
    ToolCallEnd {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
    },
    #[serde(rename = "TOOL_CALL_RESULT")]
    ToolCallResult {
        #[serde(rename = "toolCallId")]
        tool_call_id: String,
        content: String,
    },
    #[serde(rename = "STATE_SNAPSHOT")]
    StateSnapshot { snapshot: JsonValue },
    #[serde(rename = "STATE_DELTA")]
    StateDelta { delta: JsonValue },
    #[serde(rename = "RUN_ERROR")]
    RunError { message: String },
}

impl Laser {
    /// Publish an AG-UI state snapshot: the full shared state as a
    /// `state_snapshot` event (`agdx.ct = json`). Replaying a snapshot plus the
    /// later deltas reconstructs the state at any historical offset, the
    /// log-native form of AG-UI's STATE_SNAPSHOT/STATE_DELTA, over Iggy's own
    /// transport rather than SSE.
    pub async fn publish_state_snapshot(
        &self,
        topic: AgentTopic<'static>,
        source: AgentId,
        conversation: ConversationId,
        state: &JsonValue,
    ) -> Result<(), LaserError> {
        let body =
            serde_json::to_vec(state).map_err(|error| LaserError::Codec(error.to_string()))?;
        self.agdx(topic, source, conversation.into())
            .emit(body)
            .with_operation(OPERATION_STATE_SNAPSHOT)
            .content_type(ContentType::Json)
            .send()
            .await?;
        Ok(())
    }

    /// Publish an AG-UI state delta: an RFC 6902 JSON Patch document as a
    /// `state_delta` event (`agdx.ct = json`).
    pub async fn publish_state_delta(
        &self,
        topic: AgentTopic<'static>,
        source: AgentId,
        conversation: ConversationId,
        patch: &JsonValue,
    ) -> Result<(), LaserError> {
        let body =
            serde_json::to_vec(patch).map_err(|error| LaserError::Codec(error.to_string()))?;
        self.agdx(topic, source, conversation.into())
            .emit(body)
            .with_operation(OPERATION_STATE_DELTA)
            .content_type(ContentType::Json)
            .send()
            .await?;
        Ok(())
    }

    /// Reconstruct shared state by replaying `conversation`'s `state_snapshot` /
    /// `state_delta` events on `topic`: take the latest snapshot, then apply
    /// every delta after it (RFC 6902). `None` until a snapshot exists.
    pub async fn reconstruct_state(
        &self,
        conversation: ConversationId,
        topic: AgentTopic<'static>,
    ) -> Result<Option<JsonValue>, LaserError> {
        let messages = ContextAssembler::builder()
            .conversation_id(conversation)
            .topics(vec![topic])
            .build()
            .assemble(self)
            .await?;
        let mut state: Option<JsonValue> = None;
        for message in &messages {
            let Some(envelope) = &message.envelope else {
                continue;
            };
            if envelope.kind != AgentKind::Event {
                continue;
            }
            match envelope.operation.as_deref() {
                Some(OPERATION_STATE_SNAPSHOT) => {
                    state = Some(
                        serde_json::from_slice(&envelope.body)
                            .map_err(|error| LaserError::Codec(error.to_string()))?,
                    );
                }
                Some(OPERATION_STATE_DELTA) => {
                    if let Some(document) = state.as_mut() {
                        let patch: json_patch::Patch = serde_json::from_slice(&envelope.body)
                            .map_err(|error| LaserError::Codec(error.to_string()))?;
                        json_patch::patch(document, &patch)
                            .map_err(|error| LaserError::Invalid(error.to_string()))?;
                    }
                }
                _ => {}
            }
        }
        Ok(state)
    }

    /// Render `conversation` on `topic` as AG-UI events by reading the log:
    /// chat/reasoning chunk streams become `TEXT_MESSAGE_*` events, state events
    /// become `STATE_SNAPSHOT`/`STATE_DELTA`, an error terminal becomes
    /// `RUN_ERROR`. Log-native, replayable, over Iggy rather than SSE.
    pub async fn agui_events(
        &self,
        conversation: ConversationId,
        topic: AgentTopic<'static>,
    ) -> Result<Vec<AgUiEvent>, LaserError> {
        let messages = ContextAssembler::builder()
            .conversation_id(conversation)
            .topics(vec![topic])
            .build()
            .assemble(self)
            .await?;
        // The chunk-stream purpose rides only the opening chunk (sequence 0), so
        // track each channel's kind as the stream opens and reuse it for the
        // later chunks (a terminal chunk carries no purpose).
        let mut channels: std::collections::HashMap<String, ChunkKind> =
            std::collections::HashMap::new();
        let mut events = Vec::new();
        for message in &messages {
            let Some(envelope) = &message.envelope else {
                continue;
            };
            if envelope.kind == AgentKind::Chunk {
                let id = envelope
                    .channel
                    .map(|channel| channel.to_string())
                    .unwrap_or_default();
                let kind = if envelope.sequence == Some(0) {
                    let kind = chunk_kind_of(envelope);
                    channels.insert(id.clone(), kind);
                    kind
                } else {
                    channels.get(&id).copied().unwrap_or(ChunkKind::Chat)
                };
                events.extend(chunk_to_agui(envelope, kind));
            } else {
                events.extend(envelope_to_agui(envelope));
            }
        }
        Ok(events)
    }
}

// The chunk-stream purpose the opening chunk declares decides which AG-UI
// message family the stream renders as.
#[derive(Clone, Copy)]
enum ChunkKind {
    Chat,
    Reasoning,
    ToolArgs,
}

// The kind an opening chunk's purpose declares (mid-stream chunks carry none).
fn chunk_kind_of(envelope: &AgentEnvelope) -> ChunkKind {
    match envelope.operation.as_deref() {
        Some(OPERATION_REASONING) => ChunkKind::Reasoning,
        Some(OPERATION_TOOL_ARGS) => ChunkKind::ToolArgs,
        _ => ChunkKind::Chat,
    }
}

/// Translate one non-chunk AGDX envelope into the AG-UI events it represents
/// (chunks are handled by [`Laser::agui_events`], which threads the per-channel
/// stream kind).
fn envelope_to_agui(envelope: &AgentEnvelope) -> Vec<AgUiEvent> {
    match envelope.kind {
        AgentKind::Status if envelope.operation.as_deref() == Some(OPERATION_TASK) => {
            // A task lifecycle update: submitted opens the run, a terminal state
            // closes it. threadId = conversation, runId = correlation.
            let thread_id = envelope.conversation.to_string();
            let run_id = envelope
                .correlation
                .map(|correlation| correlation.to_string())
                .unwrap_or_default();
            match envelope.task_state {
                Some(TaskState::Submitted) => vec![AgUiEvent::RunStarted { thread_id, run_id }],
                Some(state) if state.is_terminal() => {
                    vec![AgUiEvent::RunFinished { thread_id, run_id }]
                }
                _ => Vec::new(),
            }
        }
        // A response/error carrying `tool` is a tool result.
        AgentKind::Response | AgentKind::Error if envelope.tool.is_some() => {
            let tool_call_id = envelope
                .correlation
                .map(|correlation| correlation.to_string())
                .unwrap_or_default();
            vec![AgUiEvent::ToolCallResult {
                tool_call_id,
                content: String::from_utf8_lossy(&envelope.body).into_owned(),
            }]
        }
        AgentKind::Error => vec![AgUiEvent::RunError {
            message: String::from_utf8_lossy(&envelope.body).into_owned(),
        }],
        AgentKind::Event => match envelope.operation.as_deref() {
            Some(OPERATION_STATE_SNAPSHOT) => serde_json::from_slice(&envelope.body)
                .map(|snapshot| vec![AgUiEvent::StateSnapshot { snapshot }])
                .unwrap_or_default(),
            Some(OPERATION_STATE_DELTA) => serde_json::from_slice(&envelope.body)
                .map(|delta| vec![AgUiEvent::StateDelta { delta }])
                .unwrap_or_default(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

// Render a chunk of a chat / reasoning / tool_args stream as its AG-UI message
// family. The stream's purpose is declared on the opening chunk (sequence 0),
// so a reader tracks it per channel. Here each chunk is self-describing enough
// because the opening chunk carries the purpose and later chunks of a known
// channel reuse it. The opening chunk decides the kind, and the consumer threads it.
fn chunk_to_agui(envelope: &AgentEnvelope, kind: ChunkKind) -> Vec<AgUiEvent> {
    let id = envelope
        .channel
        .map(|channel| channel.to_string())
        .unwrap_or_default();
    let body = String::from_utf8_lossy(&envelope.body).into_owned();
    let opening = envelope.sequence == Some(0);
    let mut events = Vec::new();
    match kind {
        ChunkKind::Chat => {
            if opening {
                events.push(AgUiEvent::TextMessageStart {
                    message_id: id.clone(),
                    role: "assistant".to_owned(),
                });
            }
            if !body.is_empty() {
                events.push(AgUiEvent::TextMessageContent {
                    message_id: id.clone(),
                    delta: body,
                });
            }
            if envelope.last {
                events.push(AgUiEvent::TextMessageEnd { message_id: id });
            }
        }
        ChunkKind::Reasoning => {
            if opening {
                events.push(AgUiEvent::ReasoningMessageStart {
                    message_id: id.clone(),
                    role: "reasoning".to_owned(),
                });
            }
            if !body.is_empty() {
                events.push(AgUiEvent::ReasoningMessageContent {
                    message_id: id.clone(),
                    delta: body,
                });
            }
            if envelope.last {
                events.push(AgUiEvent::ReasoningMessageEnd { message_id: id });
            }
        }
        ChunkKind::ToolArgs => {
            if opening {
                events.push(AgUiEvent::ToolCallStart {
                    tool_call_id: id.clone(),
                    tool_call_name: envelope.tool.clone().unwrap_or_default(),
                });
            }
            if !body.is_empty() {
                events.push(AgUiEvent::ToolCallArgs {
                    tool_call_id: id.clone(),
                    delta: body,
                });
            }
            if envelope.last {
                events.push(AgUiEvent::ToolCallEnd { tool_call_id: id });
            }
        }
    }
    events
}
