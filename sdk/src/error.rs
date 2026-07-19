use crate::fork::ForkError;
use crate::kv::KvError;
#[cfg(feature = "provenance")]
use crate::provenance::ProvenanceError;
use crate::query::QueryError;
use crate::types::IdError;
use iggy::prelude::IggyError;
use laser_wire::error::{DecodeError, InvalidError};
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
    /// A transient handler-side failure a handler may return to request a retry
    /// (the reliable consumer retries it under the [`RetryPolicy`](crate::agent::RetryPolicy)).
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
    /// LaserData Cloud answered a graph op with a typed failure.
    #[error("graph: {0}")]
    Graph(#[from] laser_wire::graph::GraphError),
    /// LaserData Cloud answered an agent or workflow control op with a typed failure.
    #[error("agent: {0}")]
    Agent(#[from] laser_wire::agent_workflow::AgentError),
    /// The streaming server answered an authorization op with a typed failure.
    #[error("authz: {0}")]
    Authz(#[from] laser_wire::authz::AuthzError),
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
    /// The connected infrastructure does not provide the requested feature
    /// (raw Apache Iggy without LaserData Cloud, or a deployment whose backend
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
    /// or failed verification. The authorship and control-plane authorization
    /// gate, never retryable.
    #[error("signature: {0}")]
    Signature(String),
    /// The enrolled [`ActionGovernor`](crate::govern::ActionGovernor) rejected
    /// the action before the effect ran. Never retryable: the policy, not the
    /// transport, said no.
    #[error("policy blocked: {0}")]
    PolicyBlocked(String),
    /// The enrolled governor paused the action on a stronger approval. The
    /// message names the scope the approval must grant, so a handler can run an
    /// [`approval_gate`](crate::agent::AgentCtx::approval_gate) and re-send.
    /// Never retryable as-is.
    #[error("step-up required: {0}")]
    StepUpRequired(String),
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
    /// [`InboxRoute::Fixed`](crate::agent::InboxRoute::Fixed), or routes elsewhere.
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
    /// A fenced write lost the fence: the held token is below the live sequence,
    /// so a newer holder owns the task. Not alarming and never retryable by the
    /// loser, it is the at-most-one-effective-writer gate doing its job.
    #[error("fence violation: held {stale}, current {current}")]
    FenceViolation { stale: u64, current: u64 },
    /// A spend ceiling was reached. Not retryable until the ceiling is raised.
    #[error("budget exceeded: spent {spent} of ceiling {ceiling}")]
    BudgetExceeded { ceiling: u64, spent: u64 },
    /// A registered run's recorded cancel intent was observed at a step
    /// boundary. The completed steps' compensations have run. Not retryable on
    /// the same run.
    #[error("run cancelled: {run}")]
    Cancelled { run: String },
    /// The agent was quarantined and may not act. Not retryable.
    #[error("quarantined: {agent}")]
    Quarantined { agent: String },
    #[error(transparent)]
    Iggy(#[from] IggyError),
    #[error(transparent)]
    Id(#[from] IdError),
    #[cfg(feature = "provenance")]
    #[error(transparent)]
    Provenance(#[from] ProvenanceError),
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
            ResultCode::InvalidArgument | ResultCode::VersionSkew => {
                LaserError::Invalid(error.message)
            }
            other => LaserError::Protocol(format!("{other:?}: {}", error.message)),
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
#[cfg(any(
    feature = "fork",
    feature = "graph",
    feature = "kv",
    feature = "projections",
    feature = "query",
    feature = "rbac",
    feature = "runs"
))]
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
    /// A handler returns this to reject a message permanently: the reliable
    /// consumer dead-letters it immediately instead of retrying.
    pub fn rejected(reason: impl Into<String>) -> Self {
        Self::Rejected(reason.into())
    }

    /// An unsupported-surface refusal: the whole `surface` is absent on the
    /// connected deployment (e.g. any managed call against raw Apache Iggy).
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

    /// Whether retrying the same call can succeed. Transport and backend
    /// failures are transient. Everything the caller would have to change
    /// first (rejected, unsupported, invalid input, version skew, too-large,
    /// not-found) is permanent and reports `false`.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Rejected(_)
            | Self::Unsupported { .. }
            | Self::Invalid(_)
            | Self::Codec(_)
            | Self::Protocol(_)
            | Self::Config(_)
            | Self::HandlerConfig(_)
            | Self::Integrity { .. }
            | Self::NoStream
            | Self::PolicyBlocked(_)
            | Self::StepUpRequired(_)
            | Self::NoRespondTopic
            | Self::PresenceConflict { .. }
            | Self::RoutePrincipalMismatch { .. } => false,
            Self::Query(error) => matches!(error, QueryError::Backend(_)),
            Self::Kv(error) => matches!(error, KvError::Backend(_) | KvError::NotLeader),
            Self::Fork(error) => matches!(error, ForkError::Backend(_) | ForkError::NotLeader),
            _ => true,
        }
    }

    /// Whether the connected plane declined a conditional write because it
    /// does not own that operation's mutation partition.
    pub fn is_not_leader(&self) -> bool {
        matches!(
            self,
            Self::Kv(KvError::NotLeader)
                | Self::Fork(ForkError::NotLeader)
                | Self::Agent(laser_wire::agent_workflow::AgentError::NotLeader)
        )
    }

    /// Whether the failure means the connected infrastructure (or backend)
    /// does not provide the feature at all. Permanent: do not retry, gate the
    /// code path instead.
    pub fn is_unsupported(&self) -> bool {
        matches!(
            self,
            Self::Unsupported { .. }
                | Self::Query(QueryError::Unsupported(_))
                | Self::Kv(KvError::Unsupported(_))
                | Self::Fork(ForkError::Unsupported(_))
                | Self::Graph(laser_wire::graph::GraphError::Unsupported(_))
        )
    }

    /// Whether the failure names a missing resource (index, fork). Distinct
    /// from a backend being down: the call worked, the thing is not there.
    pub fn is_not_found(&self) -> bool {
        matches!(
            self,
            Self::Query(QueryError::IndexNotFound(_) | QueryError::ForkNotFound(_))
                | Self::Fork(ForkError::NotFound(_))
        )
    }

    /// Whether the failure is wire-version skew between this SDK and the
    /// connected LaserData Cloud (the envelope's `v` was not accepted). Permanent for
    /// this client build: upgrade or downshift instead of retrying.
    pub fn is_version_skew(&self) -> bool {
        matches!(
            self,
            Self::Query(QueryError::Version { .. })
                | Self::Kv(KvError::Version { .. })
                | Self::Fork(ForkError::Version { .. })
        )
    }

    /// Whether a compare-and-swap lost its precondition (the key changed under
    /// it, or an `expect_absent` create lost a race). Not a failure to retry
    /// blindly: re-read the current value and version, recompute, and CAS again.
    pub fn is_version_conflict(&self) -> bool {
        matches!(self, Self::Kv(KvError::VersionConflict { .. }))
    }

    /// Whether a read-consistency level could not be met: the projector has not
    /// yet applied the source log up to the point a `read_your_writes` query
    /// required. Retryable: the read model is catching up.
    pub fn is_stale(&self) -> bool {
        matches!(self, Self::Query(QueryError::Stale { .. }))
    }

    /// Whether the connected server refused the operation for missing
    /// permissions (the substrate's RBAC: global permissions plus per-stream
    /// and per-topic grants). The principal exists and authenticated, the
    /// grant does not. The fix is an admin conversation, not a retry: ask for
    /// the stream or topic permission the failing verb needs (read, send,
    /// create), or point the client at a stream the principal can use.
    pub fn is_permission_denied(&self) -> bool {
        matches!(
            self,
            Self::Iggy(IggyError::Unauthorized | IggyError::Unauthenticated)
                | Self::RoutePrincipalMismatch { .. }
        )
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
            self,
            Self::Iggy(
                IggyError::StreamIdNotFound(_)
                    | IggyError::StreamNameNotFound(_)
                    | IggyError::TopicIdNotFound(_, _)
                    | IggyError::TopicNameNotFound(_, _)
            )
        )
    }

    /// Whether capability routing found no live agent for the skill.
    pub fn is_no_capable_agent(&self) -> bool {
        matches!(self, Self::NoCapableAgent { .. })
    }

    /// Whether an advisory lease was lost (expired or re-acquired by another
    /// holder). For a fenced write, see [`is_fence_violation`](Self::is_fence_violation).
    pub fn is_lease_lost(&self) -> bool {
        matches!(self, Self::Kv(KvError::LeaseLost))
    }

    /// Whether a fenced write lost the fence (a stale holder stepped aside). Not
    /// alarming, never retryable by the loser.
    pub fn is_fence_violation(&self) -> bool {
        matches!(self, Self::FenceViolation { .. })
    }

    /// Whether a spend ceiling was reached.
    pub fn is_budget_exceeded(&self) -> bool {
        matches!(self, Self::BudgetExceeded { .. })
    }

    /// Whether the agent is quarantined.
    pub fn is_quarantined(&self) -> bool {
        matches!(self, Self::Quarantined { .. })
    }

    /// This failure's place in the unified [`ResultCode`] space. A managed
    /// surface error projects onto its surface code, and a client-side failure maps
    /// to the closest code. Lets a caller branch on one dictionary instead of
    /// matching every variant, and mirrors the HTTP status the same error gets
    /// on the management surface.
    pub fn code(&self) -> ResultCode {
        match self {
            Self::Query(error) => ResultCode::from(error),
            Self::Kv(error) => ResultCode::from(error),
            Self::Fork(error) => ResultCode::from(error),
            Self::Unsupported { .. } | Self::NoStream | Self::NoRespondTopic => {
                ResultCode::Unsupported
            }
            Self::Invalid(_) | Self::Config(_) | Self::HandlerConfig(_) | Self::Rejected(_) => {
                ResultCode::InvalidArgument
            }
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
            Self::StepUpRequired(_) => ResultCode::StepUpRequired,
            // A digest mismatch is served-data corruption, a backend fault.
            Self::Integrity { .. } => ResultCode::Backend,
            Self::NoCapableAgent { .. } | Self::NoInbox { .. } => ResultCode::NotFound,
            // A transport timeout is not read-model staleness (that is
            // `QueryError::Stale`), so it classifies as a backend/availability
            // failure, keeping `code()` and `is_stale()` in agreement.
            _ => ResultCode::Backend,
        }
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
    fn given_a_transient_handler_error_when_classified_then_should_retry() {
        assert!(LaserError::Handler("upstream hiccup".to_owned()).is_retryable());
    }

    #[test]
    fn given_not_leader_when_classified_then_should_be_retryable_and_typed() {
        let error = LaserError::from(KvError::NotLeader);
        assert!(error.is_retryable());
        assert!(error.is_not_leader());
        assert_eq!(error.code(), ResultCode::Backend);
    }
}
