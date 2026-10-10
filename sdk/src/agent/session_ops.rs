use crate::agent::Session;
use crate::agent::agdx::AgdxReceipt;
use crate::context::{ContextMessage, ContextPolicy};
use crate::error::LaserError;
use crate::provenance::AgentTopic;
use crate::types::MintUlid;
use iggy::prelude::{Identifier, StreamClient, TopicClient};
use laser_wire::agent::{
    AgentErrorBody, ContextManifest, CorrelationId, Fragment, METADATA_DURATION_MICROS,
    METADATA_PROVIDER_NAME, METADATA_REQUEST_MODEL, METADATA_RESPONSE_MODEL, OPERATION_CHAT,
    OPERATION_STATE_DELTA, OPERATION_STATE_SNAPSHOT, SessionRef, StateDelta, StateSnapshot,
    TokenUsage, apply_json_patch, estimate_tokens,
};
use laser_wire::content::ContentType;
use laser_wire::dispatch::{OPERATION_CONTEXT_ASSEMBLED, OPERATION_EXECUTE_TOOL};
use laser_wire::framing::{decode_named, encode_named};
use laser_wire::graph::{ProducerInfo, SourceRef};
use laser_wire::query::Value;
use laser_wire::session::SessionStateView;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

// Keys whose values the default redaction drops from tool arguments and
// model request bodies before they are published.
const REDACTED_KEYS: [&str; 6] = [
    "authorization",
    "api_key",
    "token",
    "password",
    "secret",
    "cookie",
];

// Automatic state snapshots are written after this many deltas.
const SNAPSHOT_EVERY_DELTAS: u32 = 64;

/// Drop the values of keys such as `authorization`, `api_key`, `token`,
/// `password`, `secret`, and `cookie`, at any depth. A convenience, not a
/// guarantee: a secret under another key is published as it is.
pub fn default_redact(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(fields) => {
            for (key, field) in fields.iter_mut() {
                if REDACTED_KEYS.contains(&key.to_ascii_lowercase().as_str()) {
                    *field = serde_json::Value::String("[redacted]".to_owned());
                } else {
                    default_redact(field);
                }
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(default_redact),
        _ => {}
    }
}

// The state this handle has written: the revision it expects next, the
// document after its own deltas, and how many deltas since the last snapshot.
#[derive(Default)]
pub(crate) struct StateCursor {
    revision: u64,
    document: Option<serde_json::Value>,
    since_snapshot: u32,
}

/// One model call request. The SDK never calls a model: the application
/// calls its provider and records the call through the session.
#[derive(Debug, Clone)]
pub struct ModelRequest {
    pub model: String,
    pub provider: Option<String>,
    pub operation: String,
    pub body: Vec<u8>,
}

impl ModelRequest {
    /// A `chat` request to `model` with `body` as the prompt.
    pub fn new(model: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        Self {
            model: model.into(),
            provider: None,
            operation: OPERATION_CHAT.to_owned(),
            body: body.into(),
        }
    }

    /// The provider that serves the call.
    #[must_use]
    pub fn provider(mut self, provider: impl Into<String>) -> Self {
        self.provider = Some(provider.into());
        self
    }

    /// Another model operation, such as `text_completion`.
    #[must_use]
    pub fn operation(mut self, operation: impl Into<String>) -> Self {
        self.operation = operation.into();
        self
    }
}

/// One model call result.
#[derive(Debug, Clone, Default)]
pub struct ModelResponse {
    pub body: Vec<u8>,
    pub model: Option<String>,
    pub finish_reason: Option<String>,
    pub usage: Option<TokenUsage>,
    /// The call's duration, when the application measured it itself. Left
    /// unset, [`Session::record_model_call`] records a duration of 0.
    pub duration: Option<Duration>,
}

/// The context a model call received, assembled without writing anything.
#[derive(Debug, Clone)]
pub struct AssembledContext {
    pub fragments: Vec<ContextMessage>,
    pub manifest: ContextManifest,
}

impl AssembledContext {
    /// The fragments' payloads as UTF-8, one per line.
    pub fn text(&self) -> String {
        self.fragments
            .iter()
            .map(|fragment| String::from_utf8_lossy(&fragment.payload).into_owned())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

/// A model call in flight. Finish it with [`complete`](Self::complete) or
/// [`fail`](Self::fail).
pub struct ModelCall {
    session: Session,
    correlation: CorrelationId,
    operation: String,
    started: Instant,
}

impl ModelCall {
    /// The call's correlation, shared by its request, manifest, and result.
    pub fn correlation(&self) -> CorrelationId {
        self.correlation
    }

    /// Record the model's response, with its usage, the answering model, the
    /// finish reason, and the call duration.
    pub async fn complete(self, response: ModelResponse) -> Result<AgdxReceipt, LaserError> {
        let duration = response.duration.unwrap_or_else(|| self.started.elapsed());
        self.session
            .write_result(
                self.correlation,
                &self.operation,
                None,
                response.body,
                response.usage,
                duration,
                response.model,
                response.finish_reason,
            )
            .await
    }

    /// Record that the call failed.
    pub async fn fail(self, error: AgentErrorBody) -> Result<AgdxReceipt, LaserError> {
        self.session
            .write_failure(self.correlation, &self.operation, None, &error)
            .await
    }
}

/// A tool call in flight. Finish it with [`complete`](Self::complete) or
/// [`fail`](Self::fail).
pub struct ToolCall {
    session: Session,
    correlation: CorrelationId,
    tool: String,
    started: Instant,
}

impl ToolCall {
    /// The call's correlation, shared by its command and result.
    pub fn correlation(&self) -> CorrelationId {
        self.correlation
    }

    /// Record the tool's result and the measured duration.
    pub async fn complete(self, result: impl Into<Vec<u8>>) -> Result<AgdxReceipt, LaserError> {
        let duration = self.started.elapsed();
        self.session
            .write_result(
                self.correlation,
                OPERATION_EXECUTE_TOOL,
                Some(&self.tool),
                result.into(),
                None,
                duration,
                None,
                None,
            )
            .await
    }

    /// Record that the tool failed.
    pub async fn fail(self, error: AgentErrorBody) -> Result<AgdxReceipt, LaserError> {
        self.session
            .write_failure(
                self.correlation,
                OPERATION_EXECUTE_TOOL,
                Some(&self.tool),
                &error,
            )
            .await
    }
}

/// The session's state document: RFC 6902 patches on the session lane,
/// applied in lane order, with snapshots as checkpoints.
pub struct SessionState {
    session: Session,
}

impl SessionState {
    /// Set `key` to `value`.
    pub async fn set(
        &self,
        key: &str,
        value: serde_json::Value,
    ) -> Result<AgdxReceipt, LaserError> {
        let path = format!("/{}", key.replace('~', "~0").replace('/', "~1"));
        let operation = serde_json::from_value(serde_json::json!({
            "op": "add",
            "path": path,
            "value": value,
        }))
        .map_err(|error| LaserError::Invalid(format!("state patch: {error}")))?;
        self.patch(vec![operation]).await
    }

    /// Apply `patch` atomically against the revision this handle last saw.
    /// Append success does not prove the patch applied: a reader learns the
    /// outcome from the folded state.
    pub async fn patch(
        &self,
        patch: Vec<laser_wire::agent::PatchOperation>,
    ) -> Result<AgdxReceipt, LaserError> {
        self.seed().await?;
        let (delta, due) = {
            let mut cursor = self.session.state.lock().expect("session state lock");
            let base = cursor.document.clone().unwrap_or(serde_json::json!({}));
            let next = apply_json_patch(&base, &patch)
                .map_err(|error| LaserError::Invalid(error.to_string()))?;
            let delta = StateDelta {
                base_revision: cursor.revision,
                patch,
                op_id: ulid::Ulid::generate().to_string(),
            };
            cursor.revision += 1;
            cursor.document = Some(next);
            cursor.since_snapshot += 1;
            (delta, cursor.since_snapshot >= SNAPSHOT_EVERY_DELTAS)
        };
        let receipt = self
            .session
            .write_event(OPERATION_STATE_DELTA, encode_named(&delta)?, None)
            .await?;
        if due {
            self.snapshot().await?;
        }
        Ok(receipt)
    }

    /// Replace the whole document with `document`, a JSON object, as one
    /// revision-guarded patch that removes the keys it drops and sets every
    /// key it holds.
    pub async fn replace(&self, document: serde_json::Value) -> Result<AgdxReceipt, LaserError> {
        let serde_json::Value::Object(next) = document else {
            return Err(LaserError::Invalid(
                "a state document is a JSON object".to_owned(),
            ));
        };
        self.seed().await?;
        let current = self
            .session
            .state
            .lock()
            .expect("session state lock")
            .document
            .clone()
            .unwrap_or(serde_json::json!({}));
        let pointer = |key: &str| format!("/{}", key.replace('~', "~0").replace('/', "~1"));
        let mut operations = Vec::new();
        if let serde_json::Value::Object(current) = &current {
            for key in current.keys().filter(|key| !next.contains_key(*key)) {
                operations.push(serde_json::json!({"op": "remove", "path": pointer(key)}));
            }
        }
        for (key, value) in next {
            operations
                .push(serde_json::json!({"op": "add", "path": pointer(&key), "value": value}));
        }
        let patch = serde_json::from_value(serde_json::Value::Array(operations))
            .map_err(|error| LaserError::Invalid(format!("state patch: {error}")))?;
        self.patch(patch).await
    }

    /// Write the whole document this handle holds as a snapshot.
    pub async fn snapshot(&self) -> Result<AgdxReceipt, LaserError> {
        self.seed().await?;
        let snapshot = {
            let cursor = self.session.state.lock().expect("session state lock");
            StateSnapshot {
                base_revision: cursor.revision,
                document: cursor.document.clone().unwrap_or(serde_json::json!({})),
            }
        };
        let receipt = self
            .session
            .write_event(OPERATION_STATE_SNAPSHOT, encode_named(&snapshot)?, None)
            .await?;
        let mut cursor = self.session.state.lock().expect("session state lock");
        if cursor.revision == snapshot.base_revision {
            cursor.since_snapshot = 0;
        }
        Ok(receipt)
    }

    // A handle that has not written yet starts from the folded lane, so its
    // first delta names the revision the lane is at.
    async fn seed(&self) -> Result<(), LaserError> {
        if self
            .session
            .state
            .lock()
            .expect("session state lock")
            .document
            .is_some()
        {
            return Ok(());
        }
        let view = self.current_view().await?;
        let mut cursor = self.session.state.lock().expect("session state lock");
        if cursor.document.is_none() {
            cursor.revision = view.revision;
            cursor.document = Some(view.document);
        }
        Ok(())
    }

    // The revision a writer continues from. The lane is read straight from
    // the log, so it is never behind the writes already confirmed there. The
    // managed view is used only when the retained lane no longer holds the
    // document's baseline, and a session the index does not know yet keeps
    // the lane fold.
    pub(crate) async fn current_view(&self) -> Result<SessionStateView, LaserError> {
        let lane = self.get().await?;
        if lane.complete || !self.session.laser.capabilities().await.sessions {
            return Ok(lane);
        }
        match self
            .session
            .laser
            .sessions()
            .state(self.session.conversation(), 0)
            .await
        {
            Ok(view) if view.revision >= lane.revision => Ok(view),
            _ => Ok(lane),
        }
    }

    pub(crate) async fn snapshot_if_changed(&self) -> Result<(), LaserError> {
        let changed = self
            .session
            .state
            .lock()
            .expect("session state lock")
            .since_snapshot
            > 0;
        if changed {
            self.snapshot().await?;
        }
        Ok(())
    }

    /// Fold the state from the retained session lane: the newest snapshot
    /// plus the deltas after it. `complete` is false when the lane no longer
    /// holds the records the document starts from.
    pub async fn get(&self) -> Result<SessionStateView, LaserError> {
        let records = self
            .session
            .scope()
            .fetch_with(
                vec![AgentTopic::Sessions],
                Box::new(crate::context::LastN(usize::MAX)),
            )
            .await?;
        let mut document = serde_json::json!({});
        let mut revision = 0u64;
        let mut complete = false;
        for record in records {
            let Some(envelope) = record.envelope else {
                continue;
            };
            match envelope.operation.as_deref() {
                Some(OPERATION_STATE_SNAPSHOT) => {
                    let snapshot: StateSnapshot = decode_named(&envelope.body)?;
                    // The first snapshot is the baseline. A later one counts
                    // only at the revision the fold has reached, so a stale
                    // snapshot from a second writer changes nothing.
                    if complete && snapshot.base_revision != revision {
                        continue;
                    }
                    document = snapshot.document;
                    revision = snapshot.base_revision;
                    complete = true;
                }
                Some(OPERATION_STATE_DELTA) => {
                    let delta: StateDelta = decode_named(&envelope.body)?;
                    if delta.base_revision == 0 && revision == 0 {
                        complete = true;
                    }
                    if delta.base_revision != revision {
                        continue;
                    }
                    if let Ok(next) = apply_json_patch(&document, &delta.patch) {
                        document = next;
                        revision += 1;
                    }
                }
                _ => {}
            }
        }
        Ok(SessionStateView {
            revision,
            document,
            history: Vec::new(),
            frontier: None,
            complete,
        })
    }
}

impl Session {
    /// This session as a stream-scoped reference, for writes that land
    /// outside the session's own stream.
    pub fn reference(&self) -> Result<SessionRef, LaserError> {
        Ok(SessionRef {
            stream: self.stream()?.to_owned(),
            session: self.conversation().into(),
        })
    }

    /// Replace the redaction applied to tool arguments and model request
    /// bodies before they are published. The default drops the values of
    /// common secret keys.
    #[must_use]
    pub fn redact(
        mut self,
        redactor: impl Fn(&mut serde_json::Value) + Send + Sync + 'static,
    ) -> Self {
        self.redactor = std::sync::Arc::new(redactor);
        self
    }

    /// This handle with `source` as the record it acts on, stamped as the
    /// source of graph writes and the origin of remembered items.
    #[must_use]
    pub fn acting_on(mut self, source: SourceRef) -> Self {
        self.current = Some(source);
        self
    }

    /// Whether an operator asked to cancel this session on `agent.control`,
    /// by a cancel request or a forced cancel. Reads the retained control
    /// records of this session, so it answers on open Apache Iggy too.
    pub async fn cancel_requested(&self) -> Result<bool, LaserError> {
        let records = self
            .scope()
            .fetch_with(
                vec![AgentTopic::Control],
                Box::new(crate::context::LastN(usize::MAX)),
            )
            .await?;
        Ok(records.iter().any(|record| {
            record.envelope.as_ref().is_some_and(|envelope| {
                envelope.operation.as_deref()
                    == Some(laser_wire::dispatch::OPERATION_SESSION_CANCEL)
                    || envelope.task_state == Some(laser_wire::agent::TaskState::Canceled)
            })
        }))
    }

    /// The pause and cancel requests operators sent this session on
    /// `agent.control`, addressed to this handle's agent or to every agent.
    /// Inside a handler the runtime's control subscription keeps them
    /// current, and the first read of a session folds its retained control
    /// records. Outside a runtime every call reads the retained records. The
    /// runtime only records requests: the handler decides when to stop.
    pub async fn pending_control(&self) -> Result<crate::agent::PendingControl, LaserError> {
        Ok(self.control_state().await?.flags)
    }

    // The control state of this session, from the runtime's control book
    // when it is current, otherwise folded from the retained control records.
    pub(crate) async fn control_state(
        &self,
    ) -> Result<crate::agent::control::ControlState, LaserError> {
        let conversation = self.conversation();
        if let Some(state) = self
            .control
            .as_ref()
            .and_then(|book| book.state(conversation))
        {
            return Ok(state);
        }
        let me = self.agent.clone();
        let mut records = self
            .scope()
            .fetch_with(
                vec![AgentTopic::Control],
                Box::new(crate::context::LastN(usize::MAX)),
            )
            .await?;
        records.sort_by_key(|record| {
            (
                record.timestamp_micros,
                record.id.partition_id,
                record.id.offset,
            )
        });
        let (state, last) = crate::agent::control::fold(records.iter().filter_map(|record| {
            let request =
                crate::agent::control::control_request(record.envelope.as_ref()?, me.as_ref())?;
            Some(((record.id.partition_id, record.id.offset), request))
        }));
        Ok(match &self.control {
            Some(book) => book.install(conversation, state, last),
            None => state,
        })
    }

    /// The session's state document.
    pub fn state(&self) -> SessionState {
        SessionState {
            session: self.clone(),
        }
    }

    /// Assemble the model context under `policy` from the session lane and
    /// describe it in a manifest. Nothing is written.
    pub async fn assemble(
        &self,
        policy: Box<dyn ContextPolicy>,
    ) -> Result<AssembledContext, LaserError> {
        let name = policy.name();
        let version = policy.version();
        let fragments = self
            .scope()
            .fetch_with(vec![AgentTopic::Sessions], policy)
            .await?;
        let (stream_id, topic_id, generation) = self.lane_identity().await?;
        let mut tokens = 0u64;
        let mut bytes = 0u64;
        let mut frontier: BTreeMap<(u32, u32), u64> = BTreeMap::new();
        let manifest_fragments = fragments
            .iter()
            .map(|message| {
                let size = message.payload.len();
                tokens += estimate_tokens(size);
                bytes += size as u64;
                let position = frontier
                    .entry((topic_id, message.id.partition_id))
                    .or_insert(message.id.offset);
                *position = (*position).max(message.id.offset);
                Fragment::Message {
                    at: SourceRef::Message {
                        stream: stream_id,
                        topic: topic_id,
                        partition: message.id.partition_id,
                        offset: message.id.offset,
                        generation: Some(generation),
                        conversation: Some(self.conversation().to_string()),
                    },
                    tokens: u32::try_from(estimate_tokens(size)).unwrap_or(u32::MAX),
                    bytes: u32::try_from(size).unwrap_or(u32::MAX),
                }
            })
            .collect();
        Ok(AssembledContext {
            fragments,
            manifest: ContextManifest {
                policy: name,
                policy_version: version,
                fragments: manifest_fragments,
                tokens,
                bytes,
                frontier: frontier
                    .into_iter()
                    .map(|((topic, partition), offset)| (topic, partition, offset))
                    .collect(),
                correlation: None,
            },
        })
    }

    /// Record a model request addressed to this agent, and the context it
    /// received when `assembled` is given. Finish the returned call when the
    /// application's provider answers.
    pub async fn model(
        &self,
        request: ModelRequest,
        assembled: Option<&AssembledContext>,
    ) -> Result<ModelCall, LaserError> {
        let correlation = CorrelationId::mint();
        self.write_model_request(correlation, &request, assembled)
            .await?;
        Ok(ModelCall {
            session: self.clone(),
            correlation,
            operation: request.operation,
            started: Instant::now(),
        })
    }

    /// Record a tool call addressed to this agent, with `args` redacted.
    pub async fn tool(
        &self,
        name: impl Into<String>,
        mut args: serde_json::Value,
    ) -> Result<ToolCall, LaserError> {
        let tool = name.into();
        (self.redactor)(&mut args);
        let correlation = CorrelationId::mint();
        let agent = self.require_agent()?;
        let body = serde_json::to_vec(&args)
            .map_err(|error| LaserError::Codec(format!("tool arguments: {error}")))?;
        self.lane()?
            .command(correlation, body)
            .with_operation(OPERATION_EXECUTE_TOOL)
            .with_tool(tool.clone())
            .with_target(agent)
            .content_type(ContentType::Json)
            .send()
            .await?;
        Ok(ToolCall {
            session: self.clone(),
            correlation,
            tool,
            started: Instant::now(),
        })
    }

    /// Record a model call that already happened: the request, the context
    /// when given, and the response. `duration` is the measured call time.
    /// A response with no duration records `duration_micros = 0`.
    pub async fn record_model_call(
        &self,
        request: ModelRequest,
        response: ModelResponse,
        assembled: Option<&AssembledContext>,
    ) -> Result<AgdxReceipt, LaserError> {
        let correlation = CorrelationId::mint();
        self.write_model_request(correlation, &request, assembled)
            .await?;
        let duration = response.duration;
        self.write_result(
            correlation,
            &request.operation,
            None,
            response.body,
            response.usage,
            duration.unwrap_or_default(),
            response.model,
            response.finish_reason,
        )
        .await
    }

    /// The key-value namespace `namespace` with every write linked to this
    /// session. Feature `kv`.
    #[cfg(feature = "kv")]
    pub fn kv(&self, namespace: impl Into<String>) -> Result<crate::kv::Kv, LaserError> {
        Ok(self.laser.kv(namespace).in_session(self.reference()?))
    }

    /// The knowledge graph `name` with every upsert linked to this session,
    /// stamped with this agent as producer and the current record as source.
    /// Feature `graph`.
    #[cfg(feature = "graph")]
    pub fn linked_graph(
        &self,
        name: impl Into<String>,
    ) -> Result<crate::graph::GraphHandle<'_>, LaserError> {
        let mut graph = self
            .laser
            .graph(name)
            .in_session(self.reference()?)
            .produced_by(self.producer());
        if let Some(source) = &self.current {
            graph = graph.sourced_from(source.clone());
        }
        Ok(graph)
    }

    /// Record that `items`, recalled for `query` when there was one, entered
    /// the session's context, with each item's id and score.
    pub async fn record_retrieval(
        &self,
        query: Option<String>,
        items: &[crate::memory::MemoryItem],
    ) -> Result<AgdxReceipt, LaserError> {
        let retrieval = laser_wire::agent::ContextRetrieval {
            query,
            items: items
                .iter()
                .map(|item| (item.id.to_string(), item.score.unwrap_or_default()))
                .collect(),
        };
        self.write_event(
            laser_wire::dispatch::OPERATION_CONTEXT_RETRIEVED,
            encode_named(&retrieval)?,
            None,
        )
        .await
    }

    /// Record that a summary replaced the covered records of the session's
    /// context.
    pub async fn record_compaction(
        &self,
        compaction: laser_wire::agent::ContextCompaction,
    ) -> Result<AgdxReceipt, LaserError> {
        self.write_event(
            laser_wire::dispatch::OPERATION_CONTEXT_COMPACTED,
            encode_named(&compaction)?,
            None,
        )
        .await
    }

    /// This session's memory with every remembered item stamped with this
    /// agent as producer and the current record as origin.
    pub fn linked_memory(&self) -> crate::context_scope::ScopedMemory {
        self.memory()
            .with_lineage(self.current.clone(), Some(self.producer()))
    }

    fn producer(&self) -> ProducerInfo {
        ProducerInfo {
            name: match &self.agent {
                Some(agent) => format!("sdk:{agent}"),
                None => "sdk".to_owned(),
            },
            version: self.config().sdk_info().version.clone(),
        }
    }

    fn require_agent(&self) -> Result<laser_wire::agent::AgentId, LaserError> {
        self.agent
            .clone()
            .ok_or_else(|| LaserError::Invalid("this session handle has no agent".to_owned()))
    }

    async fn lane_identity(&self) -> Result<(u32, u32, u64), LaserError> {
        let stream = Identifier::named(self.stream()?)?;
        let client = self.laser.client();
        let stream_id = client
            .get_stream(&stream)
            .await?
            .ok_or_else(|| LaserError::Invalid("the session stream does not exist".to_owned()))?
            .id;
        let topic = client
            .get_topic(&stream, &AgentTopic::Sessions.as_identifier())
            .await?
            .ok_or_else(|| LaserError::Invalid("agent.sessions does not exist".to_owned()))?;
        Ok((stream_id, topic.id, topic.created_at.as_micros()))
    }

    async fn write_model_request(
        &self,
        correlation: CorrelationId,
        request: &ModelRequest,
        assembled: Option<&AssembledContext>,
    ) -> Result<(), LaserError> {
        let agent = self.require_agent()?;
        let mut body = request.body.clone();
        if let Ok(mut json) = serde_json::from_slice::<serde_json::Value>(&body) {
            (self.redactor)(&mut json);
            body = serde_json::to_vec(&json)
                .map_err(|error| LaserError::Codec(format!("model request: {error}")))?;
        }
        let lane = self.lane()?;
        let mut send = lane
            .command(correlation, body)
            .with_operation(request.operation.clone())
            .with_target(agent)
            .with_metadata(METADATA_REQUEST_MODEL, Value::Str(request.model.clone()));
        if let Some(provider) = &request.provider {
            send = send.with_metadata(METADATA_PROVIDER_NAME, Value::Str(provider.clone()));
        }
        send.send().await?;
        if let Some(assembled) = assembled {
            let mut manifest = assembled.manifest.clone();
            manifest.correlation = Some(correlation);
            self.write_event(
                OPERATION_CONTEXT_ASSEMBLED,
                encode_named(&manifest)?,
                Some(correlation),
            )
            .await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_result(
        &self,
        correlation: CorrelationId,
        operation: &str,
        tool: Option<&str>,
        body: Vec<u8>,
        usage: Option<TokenUsage>,
        duration: Duration,
        model: Option<String>,
        finish_reason: Option<String>,
    ) -> Result<AgdxReceipt, LaserError> {
        let agent = self.require_agent()?;
        let lane = self.lane()?;
        let mut send = lane
            .respond(correlation, body)
            .with_operation(operation)
            .with_target(agent)
            .with_metadata(
                METADATA_DURATION_MICROS,
                Value::Uint(u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)),
            );
        if let Some(tool) = tool {
            send = send.with_tool(tool);
        }
        if let Some(usage) = usage {
            send = send.with_usage(usage);
        }
        if let Some(model) = model {
            send = send.with_metadata(METADATA_RESPONSE_MODEL, Value::Str(model));
        }
        if let Some(reason) = finish_reason {
            send = send.with_finish_reason(reason);
        }
        send.send_receipt().await
    }

    async fn write_failure(
        &self,
        correlation: CorrelationId,
        operation: &str,
        tool: Option<&str>,
        error: &AgentErrorBody,
    ) -> Result<AgdxReceipt, LaserError> {
        let agent = self.require_agent()?;
        let lane = self.lane()?;
        let mut send = lane
            .fail(correlation, error)?
            .with_operation(operation)
            .with_target(agent);
        if let Some(tool) = tool {
            send = send.with_tool(tool);
        }
        send.send_receipt().await
    }

    pub(crate) async fn write_event(
        &self,
        operation: &str,
        body: Vec<u8>,
        correlation: Option<CorrelationId>,
    ) -> Result<AgdxReceipt, LaserError> {
        let lane = self.lane()?;
        let mut send = lane
            .emit(body)
            .with_operation(operation)
            .content_type(ContentType::Cbor);
        if let Some(correlation) = correlation {
            send = send.with_correlation(correlation);
        }
        send.send_receipt().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_secret_keys_at_any_depth_when_redacted_then_should_drop_only_their_values() {
        let mut args = serde_json::json!({
            "query": "status",
            "Authorization": "Bearer x",
            "nested": {"api_key": "k", "keep": 1},
            "list": [{"password": "p"}],
        });
        default_redact(&mut args);
        assert_eq!(args["query"], "status");
        assert_eq!(args["Authorization"], "[redacted]");
        assert_eq!(args["nested"]["api_key"], "[redacted]");
        assert_eq!(args["nested"]["keep"], 1);
        assert_eq!(args["list"][0]["password"], "[redacted]");
    }
}

/// A session handed to an agent: written as submitted on the lane, with a
/// command in the agent's inbox. The agent marks it working when it picks the
/// command up.
#[must_use = "a submission does nothing until send().await"]
pub struct SubmitBuilder {
    sessions: crate::agent::Sessions,
    agent: laser_wire::agent::AgentId,
    input: Vec<u8>,
    submitter: Option<laser_wire::agent::AgentId>,
    operation: String,
    label: Option<String>,
    namespace: Option<String>,
    budget: Option<laser_wire::agent::Budget>,
    tags: Vec<String>,
}

/// What a submission wrote: the session and the command's correlation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Submitted {
    pub session: crate::types::ConversationId,
    pub correlation: CorrelationId,
}

impl SubmitBuilder {
    /// The agent that submits the session. Required.
    pub fn from(mut self, submitter: impl Into<laser_wire::agent::AgentId>) -> Self {
        let submitter = submitter.into();
        self.submitter = Some(submitter);
        self
    }

    /// The command operation, `invoke_agent` unless set.
    pub fn operation(mut self, operation: impl Into<String>) -> Self {
        self.operation = operation.into();
        self
    }

    /// Name the session, deriving its id from the stream, namespace, and label.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// The namespace a labeled session id derives under.
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// The token and cost ceiling a reader compares the session's usage with.
    pub fn budget(mut self, budget: laser_wire::agent::Budget) -> Self {
        self.budget = Some(budget);
        self
    }

    /// One searchable tag.
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Write the submitted status and the command.
    pub async fn send(self) -> Result<Submitted, LaserError> {
        let submitter = self.submitter.ok_or_else(|| {
            LaserError::Invalid("a submission needs `.from(submitter)`".to_owned())
        })?;
        let stream = self.sessions.stream()?.to_owned();
        let session = match &self.label {
            Some(label) => crate::agent::derive_session_id(
                &stream,
                self.namespace.as_deref().unwrap_or_default(),
                label,
            ),
            None => crate::types::ConversationId::new(),
        };
        let start = laser_wire::agent::SessionStart {
            label: self.label,
            namespace: self.namespace,
            agent: self.agent.clone(),
            sdk: self.sessions.config().sdk_info().clone(),
            parent: None,
            root: None,
            idle_timeout_micros: Some(
                u64::try_from(self.sessions.config().idle_timeout_value().as_micros())
                    .unwrap_or(u64::MAX),
            ),
            budget: self.budget,
            tags: self.tags,
        };
        laser_wire::validate::Validate::validate(&start)?;
        let handle = self.sessions.open(session).as_agent(submitter);
        let lane = handle.lane()?;
        lane.status(laser_wire::agent::OPERATION_SESSION)
            .with_task_state(laser_wire::agent::TaskState::Submitted)
            .body(encode_named(&start)?)
            .content_type(ContentType::Cbor)
            .send()
            .await?;
        let correlation = CorrelationId::mint();
        lane.command(correlation, self.input)
            .with_operation(self.operation)
            .with_target(self.agent)
            .with_metadata(laser_wire::agent::METADATA_SUBMITTED, Value::Bool(true))
            .send()
            .await?;
        Ok(Submitted {
            session,
            correlation,
        })
    }
}

/// Operator control of one session, sent on `agent.control`. Only accounts
/// with send permission on that topic can use it.
pub struct SessionControl {
    laser: crate::laser::Laser,
    session: crate::types::ConversationId,
    operator: Option<laser_wire::agent::AgentId>,
    participants: Option<Vec<laser_wire::agent::AgentId>>,
    #[cfg(feature = "sign")]
    key: Option<crate::sign::SigningKey>,
}

impl SessionControl {
    /// The operator identity the control records carry. Required.
    #[must_use]
    pub fn as_operator(mut self, operator: impl Into<laser_wire::agent::AgentId>) -> Self {
        let operator = operator.into();
        self.operator = Some(operator);
        self
    }

    /// Sign every control record with `key`, so a verifying reader can
    /// prove which operator sent it. Feature `sign`.
    #[cfg(feature = "sign")]
    #[must_use]
    pub fn signed_by(mut self, key: crate::sign::SigningKey) -> Self {
        self.key = Some(key);
        self
    }

    /// The agents whose acknowledgments complete a pause, instead of the set
    /// [`pause`](Self::pause) reads from the session lane.
    #[must_use]
    pub fn participants(
        mut self,
        participants: impl IntoIterator<Item = impl Into<laser_wire::agent::AgentId>>,
    ) -> Self {
        self.participants = Some(participants.into_iter().map(Into::into).collect());
        self
    }

    /// Ask the session's agents to pause. The request names the agents whose
    /// acknowledgments complete the pause: the set given to
    /// [`participants`](Self::participants), or else the agents the session
    /// lane shows working on the session, the addressees of its work commands
    /// and the agents that picked it up. Every agent that receives work for
    /// the session holds it until the resume, named or not.
    pub async fn pause(&self) -> Result<AgdxReceipt, LaserError> {
        let participants = match &self.participants {
            Some(participants) => participants.clone(),
            None => crate::agent::pause::lane_participants(&self.laser, self.session).await?,
        };
        let body = serde_json::to_vec(&laser_wire::agent::SessionPauseRequest { participants })
            .map_err(|error| LaserError::Codec(format!("pause request: {error}")))?;
        self.request_with(laser_wire::dispatch::OPERATION_SESSION_PAUSE, body)
            .await
    }

    /// Ask a paused session's agents to resume.
    pub async fn resume(&self) -> Result<AgdxReceipt, LaserError> {
        self.request(laser_wire::dispatch::OPERATION_SESSION_RESUME)
            .await
    }

    /// Ask the session's agents to end it as canceled at their next boundary.
    pub async fn cancel(&self) -> Result<AgdxReceipt, LaserError> {
        self.request(laser_wire::dispatch::OPERATION_SESSION_CANCEL)
            .await
    }

    /// End the session as canceled without its agents, for a session whose
    /// agent is gone.
    pub async fn force_cancel(&self) -> Result<AgdxReceipt, LaserError> {
        let end = laser_wire::agent::SessionEnd {
            reason: Some("forced".to_owned()),
            error: None,
        };
        let producer = self.producer()?;
        let send = producer
            .status(laser_wire::agent::OPERATION_SESSION)
            .with_task_state(laser_wire::agent::TaskState::Canceled)
            .body(encode_named(&end)?)
            .content_type(ContentType::Cbor)
            .last();
        self.sign(send).send_receipt().await
    }

    async fn request(&self, operation: &str) -> Result<AgdxReceipt, LaserError> {
        self.request_with(operation, b"{}".to_vec()).await
    }

    async fn request_with(
        &self,
        operation: &str,
        body: Vec<u8>,
    ) -> Result<AgdxReceipt, LaserError> {
        let producer = self.producer()?;
        let send = producer
            .command(CorrelationId::mint(), body)
            .with_operation(operation)
            .content_type(ContentType::Json);
        self.sign(send).send_receipt().await
    }

    fn sign<'a>(&'a self, send: crate::agent::AgdxSend<'a>) -> crate::agent::AgdxSend<'a> {
        #[cfg(feature = "sign")]
        if let Some(key) = &self.key {
            return send.signed_by(key);
        }
        send
    }

    fn producer(&self) -> Result<crate::agent::Agdx, LaserError> {
        let operator = self.operator.clone().ok_or_else(|| {
            LaserError::Invalid("session control needs `.as_operator(id)`".to_owned())
        })?;
        Ok(self
            .laser
            .agdx(AgentTopic::Control, operator, self.session.into()))
    }
}

impl crate::agent::Sessions {
    /// Hand a new session to `agent` with `input` as its first command.
    pub fn submit(
        &self,
        agent: impl Into<laser_wire::agent::AgentId>,
        input: impl Into<Vec<u8>>,
    ) -> SubmitBuilder {
        SubmitBuilder {
            sessions: self.clone(),
            agent: agent.into(),
            input: input.into(),
            submitter: None,
            operation: laser_wire::dispatch::OPERATION_INVOKE_AGENT.to_owned(),
            label: None,
            namespace: None,
            budget: None,
            tags: Vec::new(),
        }
    }

    /// Operator control of the session `id` in `stream`.
    pub fn control(&self, stream: &str, id: crate::types::ConversationId) -> SessionControl {
        SessionControl {
            laser: self.laser().with_default_stream(stream),
            session: id,
            operator: None,
            participants: None,
            #[cfg(feature = "sign")]
            key: None,
        }
    }
}
