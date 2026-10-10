use crate::context::ContextAssembler;
use crate::error::LaserError;
use crate::laser::Laser;
use crate::provenance::AgentTopic;
use crate::types::ConversationId;
use laser_wire::agent::{
    AgentEnvelope, AgentId, AgentKind, OPERATION_REASONING, OPERATION_STATE_DELTA,
    OPERATION_STATE_SNAPSHOT, OPERATION_TASK, OPERATION_TOOL_ARGS, TaskState,
};
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
    /// Publish an AG-UI state snapshot: replace the session state document of
    /// `conversation` with `state`, a JSON object, then snapshot it. The state
    /// rides the session lane as one revision-guarded patch, the same records
    /// `Session::state` writes, so every reader folds one state model.
    pub async fn publish_state_snapshot(
        &self,
        source: impl Into<AgentId>,
        conversation: ConversationId,
        state: &JsonValue,
    ) -> Result<(), LaserError> {
        let source = source.into();
        let session = self.sessions().open(conversation).as_agent(source);
        let document = session.state();
        document.replace(state.clone()).await?;
        document.snapshot().await?;
        Ok(())
    }

    /// Publish an AG-UI state delta: apply `patch`, an RFC 6902 JSON Patch
    /// array, to the session state document of `conversation`.
    pub async fn publish_state_delta(
        &self,
        source: impl Into<AgentId>,
        conversation: ConversationId,
        patch: &JsonValue,
    ) -> Result<(), LaserError> {
        let source = source.into();
        let operations = serde_json::from_value(patch.clone())
            .map_err(|error| LaserError::Invalid(format!("state patch: {error}")))?;
        self.sessions()
            .open(conversation)
            .as_agent(source)
            .state()
            .patch(operations)
            .await?;
        Ok(())
    }

    /// The session state document of `conversation`: the fold of the retained
    /// session lane, or the managed state view when the lane no longer holds
    /// the document's baseline. `None` until a state record exists.
    pub async fn reconstruct_state(
        &self,
        conversation: ConversationId,
    ) -> Result<Option<JsonValue>, LaserError> {
        let view = self
            .sessions()
            .open(conversation)
            .state()
            .current_view()
            .await?;
        Ok((view.revision > 0 || view.complete).then_some(view.document))
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
                Some(TaskState::Failed | TaskState::Rejected) => {
                    let detail = envelope
                        .metadata
                        .as_ref()
                        .and_then(|metadata| metadata.get("detail"))
                        .and_then(|value| match value {
                            laser_wire::query::Value::Str(detail) => Some(detail.clone()),
                            _ => None,
                        })
                        .unwrap_or_else(|| "task failed".to_owned());
                    vec![AgUiEvent::RunError { message: detail }]
                }
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
            Some(OPERATION_STATE_SNAPSHOT) => laser_wire::framing::decode_named::<
                laser_wire::agent::StateSnapshot,
            >(&envelope.body)
            .map(|snapshot| {
                vec![AgUiEvent::StateSnapshot {
                    snapshot: snapshot.document,
                }]
            })
            .unwrap_or_default(),
            Some(OPERATION_STATE_DELTA) => {
                laser_wire::framing::decode_named::<laser_wire::agent::StateDelta>(&envelope.body)
                    .ok()
                    .and_then(|delta| serde_json::to_value(delta.patch).ok())
                    .map(|delta| vec![AgUiEvent::StateDelta { delta }])
                    .unwrap_or_default()
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::agent::{ConversationId as WireConversationId, RecordId, TaskState};
    use laser_wire::query::Value;

    #[test]
    fn given_failed_task_status_when_rendered_then_should_emit_run_error() {
        let mut envelope = AgentEnvelope::status(
            RecordId::from_u128(1),
            WireConversationId::from_u128(2),
            "worker".parse().expect("valid agent id"),
            OPERATION_TASK,
        )
        .with_task_state(TaskState::Failed);
        envelope.metadata = Some(std::collections::BTreeMap::from([(
            "detail".to_owned(),
            Value::Str("worker failed".to_owned()),
        )]));

        assert!(matches!(
            envelope_to_agui(&envelope).as_slice(),
            [AgUiEvent::RunError { message }] if message == "worker failed"
        ));
    }
}
