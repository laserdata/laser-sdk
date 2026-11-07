use crate::fork::ForkError;
use crate::kv::KvError;
#[cfg(feature = "provenance")]
use crate::provenance::ProvenanceError;
use crate::query::QueryError;
use crate::types::IdError;
use iggy::prelude::{IggyError, IggyMessage, SendMessagesConfirmationResponse};
use laser_wire::error::{DecodeError, InvalidError};
use laser_wire::filter::FilterErrorReason;
use laser_wire::result::{CommandError, ResultCode};

/// The one error type every fallible SDK call returns.
///
/// LaserData-Cloud-served surfaces fail with their typed wire error nested intact
/// ([`Query`](Self::Query), [`Kv`](Self::Kv), [`Fork`](Self::Fork)), so callers
/// can match on the structured cause (`QueryError::TooLarge { cap, .. }`,
/// `KvError::InvalidKey`, ...) instead of parsing strings. Client-side failures
/// keep their own variants: [`Codec`](Self::Codec) (encode/decode),
/// [`Invalid`](Self::Invalid) (validation), [`Protocol`](Self::Protocol)
/// (unexpected reply shape). The classifier methods ([`is_retryable`](Self::is_retryable),
/// [`is_unsupported`](Self::is_unsupported), [`is_not_found`](Self::is_not_found))
/// answer the common questions without matching variants.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LaserError {
    /// A fluent or direct publish that gave up. Carries the cause, the
    /// target, the ranges that committed, and the records left unconfirmed.
    #[error(transparent)]
    PublishFailed(Box<PublishFailure>),
    /// A transient handler-side failure a handler may return to request a retry
    /// (the reliable consumer retries it under the `RetryPolicy`).
    /// Deterministic wiring, startup, and configuration failures use
    /// [`HandlerConfig`](Self::HandlerConfig) instead, which never retries.
    #[error("handler error: {0}")]
    Handler(String),
    /// A deterministic handler wiring, startup, or configuration failure: a task
    /// that stopped before becoming ready, a join failure, an invalid inbox topic
    /// or task id, a missing correlation or agent id. Retrying cannot change the
    /// outcome, so it is never retryable (the reliable consumer dead-letters it at
    /// once rather than burning the retry budget).
    #[error("handler configuration error: {0}")]
    HandlerConfig(String),
    #[error("rejected: {0}")]
    Rejected(String),
    #[error("timed out waiting for {0}")]
    Timeout(&'static str),
    /// A managed mutation's outcome is unknown: the attempt timed out or the
    /// transport failed after dispatch, so the server may or may not have
    /// applied it. This is deliberately not generically retryable. The caller
    /// must inspect [`is_ambiguous_mutation`](Self::is_ambiguous_mutation) and
    /// follow the operation-specific recovery contract. `operation_id` is the
    /// identity the mutation was sent under, when it had one: repeating the
    /// mutation under it returns the recorded outcome instead of applying the
    /// change a second time.
    #[error("managed mutation outcome unknown: {message}")]
    AmbiguousMutation {
        operation_id: Option<u128>,
        message: String,
    },
    #[error("the agent has no respond_on topic configured")]
    NoRespondTopic,
    #[error("state store: {0}")]
    StateStore(String),
    #[error("invalid configuration: {0}")]
    Config(&'static str),
    #[error(
        "no default stream set: address one explicitly (laser.stream(name).topic(name)) or pin a default with Laser::connect_with_stream / with_default_stream. Under the substrate's RBAC the stream must also be one this principal is permitted to use"
    )]
    NoStream,
    /// LaserData Cloud answered a query/browse/control op with a typed failure.
    #[error("query: {0}")]
    Query(#[from] QueryError),
    /// LaserData Cloud answered a key-value op with a typed failure.
    #[error("kv: {0}")]
    Kv(#[from] KvError),
    /// LaserData Cloud answered a fork op with a typed failure.
    #[error("fork: {0}")]
    Fork(#[from] ForkError),
    /// LaserData Cloud answered a session read with a typed failure.
    #[error("session: {0}")]
    Session(#[from] laser_wire::session::SessionError),
    /// LaserData Cloud answered a graph op with a typed failure.
    #[error("graph: {0}")]
    Graph(#[from] laser_wire::graph::GraphError),
    /// The streaming server answered an authorization op with a typed failure.
    #[error("authz: {0}")]
    Authz(#[from] laser_wire::authz::AuthzError),
    /// The streaming server or the filter catalog answered a consumer-filter
    /// operation with a typed failure.
    #[error("filter: {0}")]
    Filter(#[from] laser_wire::filter::FilterError),
    /// A filtered read stopped in front of a record the filter could not
    /// evaluate under the `stop` fault policy. The record and everything after
    /// it stay unacknowledged until the reader or the group consumer skips it.
    #[error("filter fault on partition {partition_id} at offset {offset}: {reason}")]
    FilterFault {
        partition_id: u32,
        offset: u64,
        reason: laser_wire::filter::FaultReason,
    },
    /// A matching record alone exceeds the filtered reply cap, so no page can
    /// carry it. The record and everything after it stay unacknowledged until
    /// the reader or the group consumer skips it.
    #[error(
        "the record at offset {offset} on partition {partition_id} exceeds the filtered reply cap"
    )]
    FilterOversizedRecord { partition_id: u32, offset: u64 },
    /// The consumer group was created, or already existed, but its filter
    /// could not be configured. The group stays as it is, unbound unless it
    /// ran a policy before, and no reader joined it. `source` is the typed
    /// catalog refusal or transport failure.
    #[error(
        "consumer group {group_id} ({name}) exists but its filter was not configured: {source}"
    )]
    ConsumerGroupSetup {
        group_id: u32,
        name: String,
        identity: laser_wire::filter::FilterGroupIdentity,
        source: Box<LaserError>,
    },
    /// A client-side encode or decode failed (payload codec, wire envelope,
    /// reply bytes). Deterministic for a given input, so never retryable.
    #[error("codec: {0}")]
    Codec(String),
    /// Client-side validation rejected the input before any round-trip
    /// (size caps, reserved keys, malformed identifiers).
    #[error("invalid: {0}")]
    Invalid(String),
    /// The reply decoded but carried an outcome the request cannot accept
    /// (e.g. a scan outcome for a get). Indicates server/client skew.
    #[error("protocol: {0}")]
    Protocol(String),
    /// A managed endpoint returned its original result code and message.
    #[error("managed command: {}", .0.message)]
    Command(CommandError),
    /// The connected infrastructure does not provide the requested feature
    /// (Apache Iggy without LaserData Cloud, or a deployment whose backend
    /// does not advertise the sub-capability). Permanent by definition: gate
    /// the code path on [`Laser::capabilities`](crate::laser::Laser) instead
    /// of retrying. `surface` names the accessor the call came through and
    /// `feature` the sub-capability when one exists, so a log line says what
    /// was refused without parsing prose. Build with
    /// [`unsupported`](Self::unsupported) /
    /// [`unsupported_feature`](Self::unsupported_feature).
    #[error("{}", unsupported_message(surface, *feature, message))]
    Unsupported {
        /// The surface the refused call came through (`"query"`, `"kv"`, ...).
        surface: &'static str,
        /// The sub-capability within the surface, when one exists
        /// (`"cas_fenced"`, `"keyword"`, `"watch"`, ...).
        feature: Option<&'static str>,
        /// What was asked and why it was refused.
        message: String,
    },
    /// A claim-checked body failed its digest check: the blob store returned
    /// bytes whose SHA-256 does not match the [`BodyRef`] capsule's. Never
    /// surfaced with the fetched bytes attached, so an integrity failure can
    /// never be mistaken for the body.
    ///
    /// [`BodyRef`]: laser_wire::agent::BodyRef
    #[error("integrity: the body at `{reference}` does not match its digest")]
    Integrity {
        /// The capsule reference whose bytes failed verification.
        reference: String,
    },
    /// An envelope signature was absent where required, enrolled to no known key,
    /// or failed verification. The authorship and authorization gate, never
    /// retryable.
    #[error("signature: {0}")]
    Signature(String),
    /// The enrolled `ActionGovernor` rejected
    /// the action before the effect ran. Never retryable: the policy, not the
    /// transport, said no.
    #[error("policy blocked: {0}")]
    PolicyBlocked(String),
    /// The enrolled governor paused the action on a stronger approval.
    /// `scope` names the scope the approval must grant, so a handler can run an
    /// `approval_gate` and re-send.
    /// Never retryable as-is.
    #[error("step-up required: {scope}")]
    StepUpRequired { scope: String },
    /// The enrolled governor held the action for later execution. Retryable by
    /// definition: later is the point.
    #[error("policy deferred: {0}")]
    PolicyDeferred(String),
    /// Capability routing found no live agent advertising `skill`. The caller
    /// retries after the registry refreshes, or routes elsewhere.
    #[error("no capable agent for skill: {skill}")]
    NoCapableAgent { skill: String },
    /// An advertised inbox route resolved a capable agent but the agent advertises
    /// no inbox topic in its live presence, so there is nowhere to address it. The
    /// caller waits for the agent to advertise, supplies an explicit
    /// `InboxRoute::Fixed`, or routes elsewhere.
    /// Never falls back to a shared topic name.
    #[error("agent has no advertised inbox: {agent}")]
    NoInbox { agent: String },
    /// One physical connection may advertise one logical agent. A second agent
    /// would overwrite connection metadata and steal the first agent's route.
    #[error("connection already advertises agent {advertised}, cannot advertise {requested}")]
    PresenceConflict {
        advertised: String,
        requested: String,
    },
    /// A route selected an agent claim whose live connection is not bound to the
    /// authenticated principal the caller required.
    #[error("agent {agent} is not bound to principal {expected} (actual: {actual:?})")]
    RoutePrincipalMismatch {
        agent: String,
        expected: u32,
        actual: Option<u32>,
    },
    /// A fenced write lost the fence: the `held` token is below the `current`
    /// sequence, so a newer holder owns the task. Not alarming and never
    /// retryable by the loser, it is the at-most-one-effective-writer gate
    /// doing its job.
    #[error("fence violation: held {held}, current {current}")]
    FenceViolation { held: u64, current: u64 },
    /// A spend ceiling was reached. Not retryable until the ceiling is raised.
    #[error("budget exceeded: spent {spent} of ceiling {ceiling}")]
    BudgetExceeded { ceiling: u64, spent: u64 },
    /// A registered run's recorded cancel intent was observed at a step
    /// boundary. The completed steps' compensations have run. Not retryable on
    /// the same run.
    #[error("run cancelled: {run}")]
    Cancelled { run: String },
    /// A workflow step did not complete. `reason` says how, and only a step
    /// that timed out is retryable. The completed steps' compensations have
    /// run.
    #[cfg(feature = "agent")]
    #[error("workflow run {run} step `{step}` failed: {reason}")]
    StepFailed {
        run: String,
        step: String,
        reason: crate::agent::StepFailure,
    },
    /// A workflow resume found the run's journal shortened by retention, so
    /// the steps the run completed cannot be known. Not retryable on the same
    /// run.
    #[cfg(feature = "agent")]
    #[error("workflow run {run} journal no longer holds the run's first record")]
    JournalTruncated { run: String },
    /// `begin` named a session that already ended, or `resume` named one an
    /// agent or an operator canceled. `status` is `completed`, `failed`, or
    /// `canceled`. A completed or failed session reopens through `resume`.
    #[cfg(feature = "agent")]
    #[error("session {session} already ended as {status}")]
    SessionEnded { session: String, status: String },
    /// The agent was quarantined and may not act. Not retryable.
    #[error("quarantined: {agent}")]
    Quarantined { agent: String },
    /// The Apache Iggy transport under the SDK failed, or the streaming server
    /// refused the call with its own error code.
    #[error(transparent)]
    Transport(TransportError),
    #[error(transparent)]
    Id(#[from] IdError),
    #[cfg(feature = "provenance")]
    #[error(transparent)]
    Provenance(#[from] ProvenanceError),
}

/// What a failed publish left behind. [`LaserError::publish_cause`] reaches
/// the original error, so every classifier answers for it.
#[derive(Debug, thiserror::Error)]
#[error("publish failed to {stream}/{topic}: {source}")]
#[non_exhaustive]
pub struct PublishFailure {
    #[source]
    pub source: LaserError,
    pub stream: String,
    pub topic: String,
    /// The ranges confirmed before the failure. Never replay them.
    pub committed: Vec<SendMessagesConfirmationResponse>,
    /// Records without a confirmation: those whose acknowledgement was lost
    /// and those never sent. They keep the ids the attempts used, so a resend
    /// stays deduplicated on the server.
    pub unconfirmed: Vec<IggyMessage>,
}

/// A failure of the Apache Iggy transport: a lost or refused link, or a
/// refusal the streaming server answered with its own error code.
/// [`iggy_error_code`](Self::iggy_error_code) is the stable number to branch
/// on. [`as_iggy`](Self::as_iggy) is the client error at the exact Apache
/// Iggy version this SDK builds against, the same escape hatch as
/// `laser_sdk::iggy`.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct TransportError(pub(crate) IggyError);

impl TransportError {
    /// The Apache Iggy error code, read through a failed producer send.
    pub fn iggy_error_code(&self) -> u32 {
        send_cause(&self.0).as_code()
    }

    /// The Apache Iggy client error underneath.
    pub fn as_iggy(&self) -> &IggyError {
        &self.0
    }
}

impl From<IggyError> for LaserError {
    fn from(error: IggyError) -> Self {
        Self::Transport(TransportError(error))
    }
}

// Transport failures a reconnect can clear. A failed producer send is
// classified by its cause.
pub(crate) fn is_transient_iggy_io_error(error: &IggyError) -> bool {
    match error {
        IggyError::Disconnected
        | IggyError::TcpError
        | IggyError::QuicError
        | IggyError::StaleClient
        | IggyError::NotConnected
        | IggyError::ConnectionClosed
        | IggyError::CannotEstablishConnection
        | IggyError::CannotSendMessagesDueToClientDisconnection
        | IggyError::TransientNotAccepted
        | IggyError::TransientNotCommitted
        | IggyError::CannotReadFile
        | IggyError::CannotReadPartitions => true,
        IggyError::ProducerSendFailed { cause, .. } => is_transient_iggy_io_error(cause),
        _ => false,
    }
}

// The Iggy driver ends a call it wrote and got no reply for with an unknown
// outcome, so the request may have applied.
#[cfg(feature = "streaming")]
pub(crate) fn driver_wrote(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::TransientNotCommitted
            | IggyError::OperationUncertain(_)
            | IggyError::RequestAlreadyApplied
    )
}

// The Iggy driver refuses a call these ways only before its frame reached a
// connection, so the request cannot have applied.
#[cfg(feature = "kv")]
pub(crate) fn driver_never_wrote(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::NotConnected | IggyError::ClientShutdown | IggyError::TransientNotAccepted
    )
}

// A publish answered `Unauthenticated` on a connection that already
// authenticated once means the client's socket was re-dialed underneath it,
// by its own reconnect or by a leader move, and the new node holds no session
// for it. Recovery re-dials through the connection string, which carries the
// auto-login credentials, so the retry runs authenticated.
#[cfg(feature = "streaming")]
pub(crate) fn needs_reauthentication(error: &IggyError) -> bool {
    match error {
        IggyError::Unauthenticated => true,
        IggyError::ProducerSendFailed { cause, .. } => needs_reauthentication(cause),
        _ => false,
    }
}

// The failure under any failed producer send.
fn send_cause(error: &IggyError) -> &IggyError {
    let mut cause = error;
    while let IggyError::ProducerSendFailed { cause: nested, .. } = cause {
        cause = nested;
    }
    cause
}

// The wire crate's codec failures map onto the SDK's own variants so every
// call site keeps its error shape after a `?`.
impl From<DecodeError> for LaserError {
    fn from(error: DecodeError) -> Self {
        match error {
            DecodeError::MissingPayload(message) => LaserError::Config(message),
            other => LaserError::Codec(other.to_string()),
        }
    }
}

impl From<InvalidError> for LaserError {
    fn from(error: InvalidError) -> Self {
        LaserError::Invalid(error.0)
    }
}

// An envelope rejected by the AGDX validity matrix at publish time: validation,
// so the caller fixes the envelope, never retries.
impl From<laser_wire::agent::ValidateError> for LaserError {
    fn from(error: laser_wire::agent::ValidateError) -> Self {
        LaserError::Invalid(error.to_string())
    }
}

// The canonical surface-agnostic reply a server sends for a command code it does
// not handle. Map its `ResultCode` onto the closest typed variant so the
// classifier methods still work (the common case is `Unsupported`).
impl From<CommandError> for LaserError {
    fn from(error: CommandError) -> Self {
        match error.code {
            ResultCode::Unsupported => LaserError::Unsupported {
                surface: "managed",
                feature: None,
                message: error.message,
            },
            ResultCode::InvalidArgument => LaserError::Invalid(error.message),
            _ => LaserError::Command(error),
        }
    }
}

/// Decode a managed reply of type `R`, falling back to the surface-agnostic
/// [`CommandError`] when the typed reply does not decode. A server that did not
/// handle the command code answers with a `CommandError` rather than this
/// surface's reply, so without the fallback that wrong-shape reply would surface
/// as an opaque codec error instead of a clean classification. The two shapes
/// are disjoint (an enum-tagged reply versus a `{code, message}` map), so the
/// fallback never misfires on a genuine reply.
#[cfg(feature = "streaming")]
pub(crate) fn decode_managed_reply<R: serde::de::DeserializeOwned>(
    payload: &[u8],
) -> Result<R, LaserError> {
    match laser_wire::framing::decode_named::<R>(payload) {
        Ok(reply) => Ok(reply),
        Err(typed_error) => match laser_wire::framing::decode_named::<CommandError>(payload) {
            Ok(command_error) => Err(command_error.into()),
            Err(_) => Err(LaserError::Codec(format!("decode reply: {typed_error}"))),
        },
    }
}

impl LaserError {
    pub fn publish_cause(&self) -> &Self {
        match self {
            Self::PublishFailed(failure) => failure.source.publish_cause(),
            error => error,
        }
    }
    /// A handler returns this to reject a message permanently: the reliable
    /// consumer dead-letters it immediately instead of retrying.
    pub fn rejected(reason: impl Into<String>) -> Self {
        Self::Rejected(reason.into())
    }

    /// An unsupported-surface refusal: the whole `surface` is absent on the
    /// connected deployment (e.g. any managed call against Apache Iggy).
    pub fn unsupported(surface: &'static str, message: impl Into<String>) -> Self {
        Self::Unsupported {
            surface,
            feature: None,
            message: message.into(),
        }
    }

    /// An unsupported-feature refusal: the surface is served but this
    /// sub-capability is not advertised (e.g. `kv.cas_fenced`, `query.keyword`).
    pub fn unsupported_feature(
        surface: &'static str,
        feature: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self::Unsupported {
            surface,
            feature: Some(feature),
            message: message.into(),
        }
    }

    /// Whether retrying the same call can succeed: a retryable [`code`](Self::code)
    /// (an unavailable store or node, a lagging read model), or a failure whose
    /// own meaning invites a retry although its code does not: a transient
    /// handler failure, a timeout, a deferred policy decision, a routing miss
    /// the registry may clear, and a workflow step that timed out.
    pub fn is_retryable(&self) -> bool {
        match self.classified() {
            Self::Handler(_)
            | Self::Timeout(_)
            | Self::PolicyDeferred(_)
            | Self::NoCapableAgent { .. }
            | Self::NoInbox { .. } => true,
            #[cfg(feature = "agent")]
            Self::StepFailed { reason, .. } => reason.is_retryable(),
            error => error.code().is_retryable(),
        }
    }

    /// Whether the failure was transient: the store was momentarily out of reach,
    /// a connection was refused or dropped, or the node could not reach its
    /// backend. The identical request may succeed on a later attempt, so this is
    /// the flag a retry loop keys on when it wants only the recoverable class
    /// rather than everything [`is_retryable`](Self::is_retryable) admits.
    pub fn is_unavailable(&self) -> bool {
        self.code().is_unavailable()
    }

    /// Whether the connected managed backend declined a conditional write because it
    /// does not own that operation's mutation partition.
    pub fn is_not_leader(&self) -> bool {
        matches!(
            self.classified(),
            Self::Kv(KvError::NotLeader) | Self::Fork(ForkError::NotLeader)
        )
    }

    /// Whether the failure means the connected infrastructure (or backend)
    /// does not provide the feature at all. Permanent: do not retry, gate the
    /// code path instead.
    pub fn is_unsupported(&self) -> bool {
        self.code() == ResultCode::Unsupported
    }

    /// Whether the failure names a missing resource (an index, fork, session,
    /// key, graph, role, stream, or a capable agent). Distinct from a backend
    /// being down: the call worked, the thing is not there.
    pub fn is_not_found(&self) -> bool {
        self.code() == ResultCode::NotFound
    }

    /// Whether the failure is wire-version skew between this SDK and the
    /// connected LaserData Cloud (the envelope's `v` was not accepted). Permanent for
    /// this client build: upgrade or downshift instead of retrying.
    pub fn is_version_skew(&self) -> bool {
        self.code() == ResultCode::VersionSkew
    }

    /// Whether a compare-and-swap lost its precondition (the key changed under
    /// it, or an `expect_absent` create lost a race). Not a failure to retry
    /// blindly: re-read the current value and version, recompute, and CAS again.
    pub fn is_version_conflict(&self) -> bool {
        matches!(self.classified(), Self::Kv(KvError::VersionConflict { .. }))
    }

    /// Whether a managed mutation may already have reached the server. A
    /// generic retry loop must stop: recovery depends on the operation. In
    /// particular, an ambiguous lease acquisition must wait through its
    /// requested TTL before a fresh acquisition, because an applied grant can
    /// otherwise conflict with itself and the reply carries no token to renew
    /// or release it.
    pub fn is_ambiguous_mutation(&self) -> bool {
        matches!(self.classified(), Self::AmbiguousMutation { .. })
    }

    /// Whether a read-consistency barrier could not be met: a query projector
    /// or key-value fold has not yet applied the required source position.
    /// Retryable: the read model is catching up.
    pub fn is_stale(&self) -> bool {
        self.code() == ResultCode::Stale
    }

    /// Whether the operation was refused for a missing or unverified
    /// credential or a missing grant: the substrate's RBAC (global permissions
    /// plus per-stream and per-topic grants), a managed surface's grant check,
    /// an unverified signature, or a policy or quarantine refusal. The fix is
    /// an admin conversation, not a retry: ask for the permission the failing
    /// verb needs, or point the client at a resource the principal can use.
    pub fn is_permission_denied(&self) -> bool {
        self.code().is_permission_denied()
    }

    /// Whether the target stream or topic does not exist FOR THIS PRINCIPAL.
    /// Under RBAC an existing stream the principal cannot see answers exactly
    /// like a missing one, so treat "not found" and "not permitted" as one
    /// user-facing question: does this principal have the stream, with the
    /// grant the verb needs? Especially relevant on deployments with
    /// server-managed dynamic streams, where the stream may simply not exist
    /// yet for this principal.
    pub fn is_stream_or_topic_not_found(&self) -> bool {
        matches!(
            self.iggy_cause(),
            Some(
                IggyError::StreamIdNotFound(_)
                    | IggyError::StreamNameNotFound(_)
                    | IggyError::TopicIdNotFound(_, _)
                    | IggyError::TopicNameNotFound(_, _)
            )
        )
    }

    // The Apache Iggy failure under any publish or group setup wrapping and
    // any failed producer send.
    fn iggy_cause(&self) -> Option<&IggyError> {
        match self.classified() {
            Self::Transport(error) => Some(send_cause(&error.0)),
            _ => None,
        }
    }

    /// Whether capability routing found no live agent for the skill.
    pub fn is_no_capable_agent(&self) -> bool {
        matches!(self.classified(), Self::NoCapableAgent { .. })
    }

    /// Whether a revocable lease was lost (expired, released, or re-acquired by another
    /// holder). For a fenced write, see [`is_fence_violation`](Self::is_fence_violation).
    pub fn is_lease_lost(&self) -> bool {
        matches!(self.classified(), Self::Kv(KvError::LeaseLost))
    }

    /// Whether a fenced write lost the fence (a stale holder stepped aside). Not
    /// alarming, never retryable by the loser.
    pub fn is_fence_violation(&self) -> bool {
        matches!(self.classified(), Self::FenceViolation { .. })
    }

    /// Whether a spend ceiling was reached.
    pub fn is_budget_exceeded(&self) -> bool {
        matches!(self.classified(), Self::BudgetExceeded { .. })
    }

    /// Whether the agent is quarantined.
    pub fn is_quarantined(&self) -> bool {
        matches!(self.classified(), Self::Quarantined { .. })
    }

    /// The Apache Iggy error code of the failure, when an Iggy error caused it.
    /// Reads through publish and group setup wrapping and a failed producer
    /// send, like the classifiers. Another SDK variant keeps its own
    /// classification.
    pub fn iggy_error_code(&self) -> Option<u32> {
        self.iggy_cause().map(IggyError::as_code)
    }

    /// The typed consumer-filter cause, when the failure is one. A group setup
    /// failure answers for the catalog refusal under it.
    pub fn filter_reason(&self) -> Option<FilterErrorReason> {
        match self.classified() {
            Self::Filter(error) => Some(error.reason),
            _ => None,
        }
    }

    /// This failure's place in the unified [`ResultCode`] space. A managed
    /// surface error projects onto its surface code, and a client-side failure maps
    /// to the closest code. Lets a caller branch on one dictionary instead of
    /// matching every variant, and mirrors the HTTP status the same error gets
    /// on the management surface.
    pub fn code(&self) -> ResultCode {
        match self.classified() {
            Self::Transport(error) => match send_cause(&error.0) {
                IggyError::ResourceNotFound(_)
                | IggyError::StreamIdNotFound(_)
                | IggyError::StreamNameNotFound(_)
                | IggyError::TopicIdNotFound(_, _)
                | IggyError::TopicNameNotFound(_, _) => ResultCode::NotFound,
                IggyError::Unauthorized => ResultCode::Forbidden,
                IggyError::Unauthenticated => ResultCode::Unauthenticated,
                cause if is_transient_iggy_io_error(cause) => ResultCode::Unavailable,
                _ => ResultCode::Backend,
            },
            Self::Filter(error) => error.code,
            Self::Command(error) => error.code,
            Self::FilterFault { .. } => ResultCode::InvalidArgument,
            Self::FilterOversizedRecord { .. } => ResultCode::TooLarge,
            Self::Query(error) => ResultCode::from(error),
            Self::Kv(error) => ResultCode::from(error),
            Self::Fork(error) => ResultCode::from(error),
            Self::Session(error) => ResultCode::from(error),
            Self::Graph(error) => ResultCode::from(error),
            Self::Authz(error) => ResultCode::from(error),
            Self::Unsupported { .. } => ResultCode::Unsupported,
            // A missing default stream or respond topic is a configuration
            // mistake, like an invalid identifier.
            Self::Invalid(_)
            | Self::Config(_)
            | Self::HandlerConfig(_)
            | Self::Rejected(_)
            | Self::NoStream
            | Self::NoRespondTopic
            | Self::Id(_) => ResultCode::InvalidArgument,
            Self::Cancelled { .. } => ResultCode::Cancelled,
            // A fenced write that lost the fence is a lost race, like a
            // compare-and-swap conflict.
            Self::FenceViolation { .. } | Self::PresenceConflict { .. } => ResultCode::Conflict,
            // An unverified signature means the author's identity is not
            // established (unauthenticated). A quarantined agent is authenticated
            // but not permitted to act (forbidden).
            Self::Signature(_) => ResultCode::Unauthenticated,
            Self::Quarantined { .. }
            | Self::PolicyBlocked(_)
            | Self::RoutePrincipalMismatch { .. } => ResultCode::Forbidden,
            Self::StepUpRequired { .. } => ResultCode::StepUpRequired,
            // A digest mismatch is served-data corruption, a backend fault.
            Self::Integrity { .. } => ResultCode::Backend,
            Self::NoCapableAgent { .. } | Self::NoInbox { .. } => ResultCode::NotFound,
            #[cfg(feature = "agent")]
            Self::StepFailed { reason, .. } => reason.code(),
            #[cfg(feature = "agent")]
            Self::JournalTruncated { .. } => ResultCode::ExpiredSnapshot,
            #[cfg(feature = "agent")]
            Self::SessionEnded { .. } => ResultCode::Conflict,
            // A transport timeout is not read-model staleness (that is a typed
            // query or KV stale reply), so it classifies as a backend failure,
            // keeping `code()` and `is_stale()` in agreement.
            _ => ResultCode::Backend,
        }
    }

    // The failure a wrapper stands for: a failed publish answers for its
    // cause, and a group setup failure for the refusal or transport failure
    // under it, so every classifier reads the same failure.
    fn classified(&self) -> &Self {
        match self {
            Self::PublishFailed(failure) => failure.source.classified(),
            Self::ConsumerGroupSetup { source, .. } => source.classified(),
            error => error,
        }
    }
}

// A remote-safe rendering of a failure, for the bridge HTTP routers. The
// `Display` text names streams, topics, hosts, and transport detail, which an
// unauthenticated JSON-RPC caller has no business seeing, so the wire carries
// only the classification and the detail stays in the local log.
#[cfg(any(feature = "a2a-bridge", feature = "mcp-bridge"))]
pub(crate) fn public_error_message(error: &LaserError) -> &'static str {
    match error.code() {
        ResultCode::InvalidArgument => "invalid request",
        ResultCode::Unsupported => "unsupported operation",
        ResultCode::Conflict => "conflict",
        ResultCode::Unauthenticated => "unauthenticated",
        ResultCode::Forbidden => "forbidden",
        ResultCode::StepUpRequired => "step-up authorization required",
        ResultCode::NotFound => "not found",
        _ => "internal error",
    }
}

// The `Unsupported` display: a surface refusal reads `unsupported: kv: ...`,
// a feature refusal names the capability that would serve it and where, so
// the fix ships inside the message.
fn unsupported_message(surface: &str, feature: Option<&str>, message: &str) -> String {
    match feature {
        // An accessor that is its own capability (watch) reads once, not twice.
        Some(feature) if feature == surface => format!(
            "unsupported: {surface}: {message} \
             (the `{feature}` capability serves this on LaserData Cloud)"
        ),
        Some(feature) => format!(
            "unsupported: {surface}.{feature}: {message} \
             (the `{feature}` capability serves this on LaserData Cloud)"
        ),
        None => format!("unsupported: {surface}: {message}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::Identifier;
    use laser_wire::authz::AuthzError;
    use laser_wire::filter::{FaultReason, FilterError, FilterGroupIdentity};
    use laser_wire::graph::GraphError;
    use laser_wire::session::SessionError;

    const CLASSIFICATION_CORPUS: &str =
        include_str!("../../wire/fixtures/error_classification_cases.json");

    #[cfg(all(feature = "streaming", feature = "provenance"))]
    #[test]
    fn given_the_shared_classification_corpus_when_classified_then_should_answer_every_case() {
        let corpus: ClassificationCorpus =
            serde_json::from_str(CLASSIFICATION_CORPUS).expect("the corpus parses");
        let mut mismatches = Vec::new();
        for case in corpus.cases {
            let error = case.failure.into_error();
            let code = format!("{:?}", error.code());
            if code != case.code {
                mismatches.push(format!(
                    "{}: code {code}, expected {}",
                    case.name, case.code
                ));
            }
            for classifier in &corpus.classifiers {
                let expected = case.flags.contains(classifier);
                if classify(&error, classifier) != expected {
                    mismatches.push(format!("{}: {classifier} should be {expected}", case.name));
                }
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    #[test]
    fn given_authorization_receipt_refusals_when_classified_then_should_preserve_the_wire_codes() {
        for (detail, expected) in [
            (
                AuthzError::MutationConflict("taken".to_owned()),
                ResultCode::Conflict,
            ),
            (
                AuthzError::ResourceLimit("full".to_owned()),
                ResultCode::ResourceLimit,
            ),
        ] {
            let error = LaserError::from(detail);
            assert_eq!(error.code(), expected);
            assert!(!error.is_retryable());
        }
    }

    #[test]
    fn given_managed_command_errors_when_classified_then_should_preserve_the_original_codes() {
        for (code, retryable, unavailable, denied, missing, skew) in [
            (ResultCode::Conflict, false, false, false, false, false),
            (
                ResultCode::TargetUnavailable,
                true,
                true,
                false,
                false,
                false,
            ),
            (ResultCode::Forbidden, false, false, true, false, false),
            (ResultCode::NotFound, false, false, false, true, false),
            (ResultCode::VersionSkew, false, false, false, false, true),
            (
                ResultCode::Unrecognized(42),
                false,
                false,
                false,
                false,
                false,
            ),
        ] {
            let error = LaserError::from(CommandError::new(code, "refused"));
            assert_eq!(error.code(), code);
            assert_eq!(error.is_retryable(), retryable);
            assert_eq!(error.is_unavailable(), unavailable);
            assert_eq!(error.is_permission_denied(), denied);
            assert_eq!(error.is_not_found(), missing);
            assert_eq!(error.is_version_skew(), skew);
        }
    }

    #[test]
    fn given_a_failed_producer_send_when_classified_then_should_use_its_cause() {
        for (cause, code, retryable, denied, missing) in [
            (
                IggyError::Unauthorized,
                ResultCode::Forbidden,
                false,
                true,
                false,
            ),
            (
                IggyError::Unauthenticated,
                ResultCode::Unauthenticated,
                false,
                true,
                false,
            ),
            (
                IggyError::StreamNameNotFound("deleted".into()),
                ResultCode::NotFound,
                false,
                false,
                true,
            ),
            (
                IggyError::Disconnected,
                ResultCode::Unavailable,
                true,
                false,
                false,
            ),
            (
                IggyError::InvalidFormat,
                ResultCode::Backend,
                false,
                false,
                false,
            ),
        ] {
            let error = LaserError::from(IggyError::ProducerSendFailed {
                cause: Box::new(cause),
                failed: std::sync::Arc::new(Vec::new()),
                uncertain: 0,
                committed: std::sync::Arc::new(Vec::new()),
                stream_name: "s".into(),
                topic_name: "t".into(),
            });
            assert_eq!(error.code(), code);
            assert_eq!(error.is_retryable(), retryable);
            assert_eq!(error.is_permission_denied(), denied);
            assert_eq!(error.is_not_found(), missing);
            assert_eq!(error.is_stream_or_topic_not_found(), missing);
        }
    }

    #[test]
    fn given_an_iggy_failure_when_reading_its_code_then_should_unwrap_the_send_cause() {
        let send = LaserError::from(IggyError::ProducerSendFailed {
            cause: Box::new(IggyError::Unauthorized),
            failed: std::sync::Arc::new(Vec::new()),
            uncertain: 0,
            committed: std::sync::Arc::new(Vec::new()),
            stream_name: "s".into(),
            topic_name: "t".into(),
        });
        assert_eq!(
            send.iggy_error_code(),
            Some(IggyError::Unauthorized.as_code())
        );
        assert_eq!(
            LaserError::from(IggyError::Disconnected).iggy_error_code(),
            Some(IggyError::Disconnected.as_code())
        );
        assert_eq!(
            LaserError::Handler("not an Iggy failure".to_owned()).iggy_error_code(),
            None
        );
    }

    #[test]
    fn given_a_deterministic_handler_config_error_when_classified_then_should_not_retry() {
        let error = LaserError::HandlerConfig("agent stopped before ready".to_owned());
        assert!(
            !error.is_retryable(),
            "a deterministic wiring failure must not burn the retry budget"
        );
        assert_eq!(error.code(), ResultCode::InvalidArgument);
    }

    #[test]
    fn given_a_missing_partition_when_classified_then_should_be_permanent() {
        let missing = IggyError::PartitionNotFound(7, Identifier::default(), Identifier::default());
        assert!(!is_transient_iggy_io_error(&missing));
        assert!(!LaserError::from(missing).is_retryable());
    }

    #[test]
    fn given_a_transient_handler_error_when_classified_then_should_retry() {
        assert!(LaserError::Handler("upstream hiccup".to_owned()).is_retryable());
    }

    #[test]
    fn given_not_leader_when_classified_then_should_be_retryable_and_typed() {
        let error = LaserError::from(KvError::NotLeader);
        assert!(error.is_retryable());
        assert!(error.is_not_leader());
        assert_eq!(error.code(), ResultCode::Unavailable);
    }

    #[test]
    fn given_a_backend_fault_when_classified_then_should_not_retry_blindly() {
        for error in [
            LaserError::from(QueryError::Backend("corrupt row".to_owned())),
            LaserError::from(KvError::Backend("constraint missing".to_owned())),
            LaserError::from(ForkError::Backend("invalid schema".to_owned())),
        ] {
            assert!(!error.is_retryable(), "{error} must be permanent");
            assert_eq!(error.code(), ResultCode::Backend);
        }
    }

    #[test]
    fn given_an_ambiguous_mutation_when_classified_then_should_require_specific_recovery() {
        let error = LaserError::AmbiguousMutation {
            operation_id: Some(7),
            message: "lease reply was lost".to_owned(),
        };
        assert!(error.is_ambiguous_mutation());
        assert!(!error.is_retryable());
    }

    #[test]
    fn given_a_stale_kv_read_when_classified_then_should_agree_with_the_result_code() {
        let error = LaserError::from(KvError::Stale {
            required: laser_wire::mutation::MutationPosition {
                topic_generation: 1,
                partition: 0,
                offset: 9,
            },
        });
        assert!(error.is_stale());
        assert_eq!(error.code(), ResultCode::Stale);
    }

    // A transient outcome classifies as retryable on every surface, and it keeps
    // its own code so a caller can tell "retry me" from an unrecoverable fault
    // without reading the message.
    #[test]
    fn given_a_temporarily_unavailable_surface_when_classified_then_should_retry() {
        for error in [
            LaserError::from(QueryError::Unavailable("dropped".to_owned())),
            LaserError::from(KvError::Unavailable("dropped".to_owned())),
            LaserError::from(ForkError::Unavailable("dropped".to_owned())),
        ] {
            assert!(error.is_retryable(), "{error} must be retryable");
            assert!(error.is_unavailable());
            assert_eq!(error.code(), ResultCode::Unavailable);
            assert!(error.code().is_retryable());
            assert!(!error.is_unsupported());
        }
    }

    #[derive(serde::Deserialize)]
    struct ClassificationCorpus {
        classifiers: Vec<String>,
        cases: Vec<ClassificationCase>,
    }

    #[derive(serde::Deserialize)]
    struct ClassificationCase {
        name: String,
        failure: Failure,
        code: String,
        flags: Vec<String>,
    }

    // A failure as the shared corpus names it, the same wire detail every
    // client decodes.
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum Failure {
        Query(QueryError),
        Kv(KvError),
        Fork(ForkError),
        Graph(GraphError),
        Session(SessionError),
        Authz(AuthzError),
        Filter(FilterError),
        Command(CommandError),
        Transport(u32),
        Local(LocalFailure),
        PublishFailed(Box<Failure>),
        ConsumerGroupSetup(Box<Failure>),
        TypedDecode(Option<Box<Failure>>),
    }

    #[derive(serde::Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum LocalFailure {
        LinkLost,
        NoStream,
        NoRespondTopic,
        Config,
        HandlerConfig,
        Handler,
        Rejected,
        Timeout,
        AmbiguousMutation,
        Unsupported,
        Invalid,
        Codec,
        Protocol,
        StateStore,
        Integrity,
        Signature,
        PolicyBlocked,
        StepUpRequired,
        PolicyDeferred,
        NoCapableAgent,
        NoInbox,
        PresenceConflict,
        RoutePrincipalMismatch,
        FenceViolation,
        BudgetExceeded,
        Cancelled,
        Quarantined,
        FilterFault,
        FilterOversizedRecord,
        Id,
        Provenance,
    }

    #[cfg(all(feature = "streaming", feature = "provenance"))]
    impl Failure {
        fn into_error(self) -> LaserError {
            match self {
                Self::Query(error) => error.into(),
                Self::Kv(error) => error.into(),
                Self::Fork(error) => error.into(),
                Self::Graph(error) => error.into(),
                Self::Session(error) => error.into(),
                Self::Authz(error) => error.into(),
                Self::Filter(error) => error.into(),
                Self::Command(error) => error.into(),
                Self::Transport(code) => LaserError::from(IggyError::from_code(code)),
                Self::Local(kind) => kind.into_error(),
                Self::PublishFailed(source) => {
                    LaserError::PublishFailed(Box::new(PublishFailure {
                        source: source.into_error(),
                        stream: "orders".to_owned(),
                        topic: "placed".to_owned(),
                        committed: Vec::new(),
                        unconfirmed: Vec::new(),
                    }))
                }
                Self::ConsumerGroupSetup(source) => LaserError::ConsumerGroupSetup {
                    group_id: 1,
                    name: "billing".to_owned(),
                    identity: FilterGroupIdentity {
                        stream_id: 1,
                        stream_created_at_micros: 1,
                        topic_id: 1,
                        topic_created_at_micros: 1,
                        group_id: 1,
                    },
                    source: Box::new(source.into_error()),
                },
                Self::TypedDecode(source) => {
                    let (position, source) = match source {
                        Some(source) => (None, source.into_error()),
                        None => (
                            Some(crate::types::MessageId::new(0, 7)),
                            LaserError::Codec("not json".to_owned()),
                        ),
                    };
                    LaserError::from(crate::typed::TypedDecodeError {
                        position,
                        source: Box::new(source),
                    })
                }
            }
        }
    }

    #[cfg(all(feature = "streaming", feature = "provenance"))]
    impl LocalFailure {
        fn into_error(self) -> LaserError {
            let text = || "refused".to_owned();
            match self {
                Self::LinkLost => LaserError::from(IggyError::Disconnected),
                Self::NoStream => LaserError::NoStream,
                Self::NoRespondTopic => LaserError::NoRespondTopic,
                Self::Config => LaserError::Config("refused"),
                Self::HandlerConfig => LaserError::HandlerConfig(text()),
                Self::Handler => LaserError::Handler(text()),
                Self::Rejected => LaserError::Rejected(text()),
                Self::Timeout => LaserError::Timeout("a reply"),
                Self::AmbiguousMutation => LaserError::AmbiguousMutation {
                    operation_id: None,
                    message: text(),
                },
                Self::Unsupported => LaserError::unsupported("kv", text()),
                Self::Invalid => LaserError::Invalid(text()),
                Self::Codec => LaserError::Codec(text()),
                Self::Protocol => LaserError::Protocol(text()),
                Self::StateStore => LaserError::StateStore(text()),
                Self::Integrity => LaserError::Integrity { reference: text() },
                Self::Signature => LaserError::Signature(text()),
                Self::PolicyBlocked => LaserError::PolicyBlocked(text()),
                Self::StepUpRequired => LaserError::StepUpRequired { scope: text() },
                Self::PolicyDeferred => LaserError::PolicyDeferred(text()),
                Self::NoCapableAgent => LaserError::NoCapableAgent { skill: text() },
                Self::NoInbox => LaserError::NoInbox { agent: text() },
                Self::PresenceConflict => LaserError::PresenceConflict {
                    advertised: "planner".to_owned(),
                    requested: "writer".to_owned(),
                },
                Self::RoutePrincipalMismatch => LaserError::RoutePrincipalMismatch {
                    agent: "planner".to_owned(),
                    expected: 7,
                    actual: Some(8),
                },
                Self::FenceViolation => LaserError::FenceViolation {
                    held: 3,
                    current: 5,
                },
                Self::BudgetExceeded => LaserError::BudgetExceeded {
                    ceiling: 10,
                    spent: 12,
                },
                Self::Cancelled => LaserError::Cancelled { run: text() },
                Self::Quarantined => LaserError::Quarantined { agent: text() },
                Self::FilterFault => LaserError::FilterFault {
                    partition_id: 1,
                    offset: 7,
                    reason: FaultReason::Malformed,
                },
                Self::FilterOversizedRecord => LaserError::FilterOversizedRecord {
                    partition_id: 1,
                    offset: 7,
                },
                Self::Id => LaserError::Id(IdError::Empty),
                Self::Provenance => LaserError::Provenance(ProvenanceError::MissingRequired(
                    laser_wire::headers::CONVERSATION_ID,
                )),
            }
        }
    }

    fn classify(error: &LaserError, classifier: &str) -> bool {
        match classifier {
            "retryable" => error.is_retryable(),
            "unavailable" => error.is_unavailable(),
            "unsupported" => error.is_unsupported(),
            "not_found" => error.is_not_found(),
            "version_skew" => error.is_version_skew(),
            "permission_denied" => error.is_permission_denied(),
            "stale" => error.is_stale(),
            "version_conflict" => error.is_version_conflict(),
            "ambiguous_mutation" => error.is_ambiguous_mutation(),
            "not_leader" => error.is_not_leader(),
            "lease_lost" => error.is_lease_lost(),
            "fence_violation" => error.is_fence_violation(),
            "budget_exceeded" => error.is_budget_exceeded(),
            "quarantined" => error.is_quarantined(),
            "no_capable_agent" => error.is_no_capable_agent(),
            "stream_or_topic_not_found" => error.is_stream_or_topic_not_found(),
            other => panic!("the corpus names an unknown classifier `{other}`"),
        }
    }
}
