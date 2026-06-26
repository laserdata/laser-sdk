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
    #[error("handler error: {0}")]
    Handler(String),
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
        "no stream set: target one explicitly (e.g. publish_on(stream, topic)) or set a default with Laser::connect_with_stream / Laser::with_stream"
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
    /// (raw Apache Iggy without LaserData Cloud). Permanent by definition.
    #[error("unsupported: {0}")]
    Unsupported(String),
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
            ResultCode::Unsupported => LaserError::Unsupported(error.message),
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
#[cfg(feature = "query")]
pub(crate) fn decode_managed_reply<R: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<R, LaserError> {
    match laser_wire::framing::decode_named::<R>(bytes) {
        Ok(reply) => Ok(reply),
        Err(typed_error) => match laser_wire::framing::decode_named::<CommandError>(bytes) {
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

    /// Whether retrying the same call can succeed. Transport and backend
    /// failures are transient. Everything the caller would have to change
    /// first (rejected, unsupported, invalid input, version skew, too-large,
    /// not-found) is permanent and reports `false`.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Rejected(_)
            | Self::Unsupported(_)
            | Self::Invalid(_)
            | Self::Codec(_)
            | Self::Protocol(_)
            | Self::Config(_)
            | Self::NoStream
            | Self::NoRespondTopic => false,
            Self::Query(error) => matches!(error, QueryError::Backend(_)),
            Self::Kv(error) => matches!(error, KvError::Backend(_)),
            Self::Fork(error) => matches!(error, ForkError::Backend(_)),
            _ => true,
        }
    }

    /// Whether the failure means the connected infrastructure (or backend)
    /// does not provide the feature at all. Permanent: do not retry, gate the
    /// code path instead.
    pub fn is_unsupported(&self) -> bool {
        matches!(
            self,
            Self::Unsupported(_)
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
    /// required. Retryable - the read model is catching up.
    pub fn is_stale(&self) -> bool {
        matches!(self, Self::Query(QueryError::Stale { .. }))
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
            Self::Unsupported(_) | Self::NoStream | Self::NoRespondTopic => ResultCode::Unsupported,
            Self::Invalid(_) | Self::Config(_) | Self::Rejected(_) => ResultCode::InvalidArgument,
            // A transport timeout is not read-model staleness (that is
            // `QueryError::Stale`), so it classifies as a backend/availability
            // failure, keeping `code()` and `is_stale()` in agreement.
            _ => ResultCode::Backend,
        }
    }
}
