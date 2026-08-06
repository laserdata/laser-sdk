// The unified result-code space. Each managed surface keeps its own typed error
// for the detail a caller needs, but every one of those errors also projects
// onto one logical `ResultCode` here, so a generic client, the HTTP status
// mapper, and a cross-language port all dispatch on one small dictionary
// instead of parsing per-surface strings. The codes are a pinned cross-repo
// contract, and an unknown code from a newer peer rides through as
// `Unrecognized` rather than failing, the same forward-compat shape the growable
// u8 dictionaries use.

use crate::agent_workflow::AgentError;
use crate::fork::ForkError;
use crate::graph::GraphError;
use crate::kv::KvError;
use crate::query::QueryError;
use serde::{Deserialize, Serialize};

/// One logical outcome code spanning query, key-value, fork, and browse. Built
/// from a surface's typed error via the `From` impls below. The typed error
/// keeps the detail, this is the shared classification.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ResultCode {
    /// The operation succeeded (no error to classify).
    Ok,
    /// The operation, or the managed surface, is not available here.
    Unsupported,
    /// A named entity (index, fork, key) does not exist.
    NotFound,
    /// The request was malformed or a field was out of range.
    InvalidArgument,
    /// A result or value exceeded a size cap.
    TooLarge,
    /// A precondition lost a race (a compare-and-swap version mismatch, a fork
    /// promote/squash conflict).
    Conflict,
    /// A consistency level could not be met within the deadline (the read model
    /// is still catching up).
    Stale,
    /// The wire op version is not accepted by this peer.
    VersionSkew,
    /// No credential, or an invalid one: the caller is not authenticated.
    Unauthenticated,
    /// The managed backend failed or was unreachable.
    Backend,
    /// Authenticated, but the grant needed for the operation is missing.
    Forbidden,
    /// Authenticated, but the operation needs a stronger authentication (a
    /// step-up) than the caller currently holds.
    StepUpRequired,
    /// The request could not be served right now, and the same request may
    /// succeed on a later attempt: the store was momentarily out of reach, a
    /// connection was refused or dropped, a concurrency conflict was aborted, or
    /// a resource limit was hit. Distinct from [`Backend`](Self::Backend), which
    /// is a fault the caller cannot retry away. This is the one code a generic
    /// client may safely retry with backoff, so it never has to parse a message
    /// to decide (see [`is_retryable`](Self::is_retryable)).
    Unavailable,
    /// A code from a newer peer this build does not name. Decodes and re-encodes
    /// byte-for-byte so an old build relays it rather than failing. Only a value
    /// outside the named range (13 and up) should ever appear here: `from_code`
    /// never produces `Unrecognized` for `0..=12`, which map to the named
    /// variants.
    Unrecognized(u16),
}

impl ResultCode {
    /// The pinned numeric code, stable across repos and language ports.
    pub const fn code(self) -> u16 {
        match self {
            ResultCode::Ok => 0,
            ResultCode::Unsupported => 1,
            ResultCode::NotFound => 2,
            ResultCode::InvalidArgument => 3,
            ResultCode::TooLarge => 4,
            ResultCode::Conflict => 5,
            ResultCode::Stale => 6,
            ResultCode::VersionSkew => 7,
            ResultCode::Unauthenticated => 8,
            ResultCode::Backend => 9,
            ResultCode::Forbidden => 10,
            ResultCode::StepUpRequired => 11,
            ResultCode::Unavailable => 12,
            ResultCode::Unrecognized(code) => code,
        }
    }

    /// The code for a pinned numeric value, where an unknown value becomes
    /// `Unrecognized` rather than an error.
    pub const fn from_code(code: u16) -> Self {
        match code {
            0 => ResultCode::Ok,
            1 => ResultCode::Unsupported,
            2 => ResultCode::NotFound,
            3 => ResultCode::InvalidArgument,
            4 => ResultCode::TooLarge,
            5 => ResultCode::Conflict,
            6 => ResultCode::Stale,
            7 => ResultCode::VersionSkew,
            8 => ResultCode::Unauthenticated,
            9 => ResultCode::Backend,
            10 => ResultCode::Forbidden,
            11 => ResultCode::StepUpRequired,
            12 => ResultCode::Unavailable,
            other => ResultCode::Unrecognized(other),
        }
    }

    /// The HTTP status this code maps to, the one mapping every surface shares,
    /// so a status need not be decided per surface or per route.
    pub const fn http_status(self) -> u16 {
        match self {
            ResultCode::Ok => 200,
            ResultCode::Unsupported => 501,
            ResultCode::NotFound => 404,
            ResultCode::InvalidArgument => 400,
            ResultCode::TooLarge => 413,
            ResultCode::Conflict => 409,
            ResultCode::Stale => 503,
            ResultCode::VersionSkew => 400,
            ResultCode::Unauthenticated => 401,
            ResultCode::Backend => 502,
            // Authenticated-but-forbidden is 403, distinct from the 401 an
            // unauthenticated caller gets. Step-up also lands on 403 unless the
            // HTTP layer has a better challenge status for the chosen scheme.
            ResultCode::Forbidden => 403,
            ResultCode::StepUpRequired => 403,
            // Retry-after territory, the same status a lagging read model gets.
            ResultCode::Unavailable => 503,
            ResultCode::Unrecognized(_) => 500,
        }
    }

    /// Whether a caller may retry the identical request and reasonably expect a
    /// different outcome. True only for the two transient classes: the store was
    /// momentarily unreachable, or the read model had not caught up yet. Every
    /// other code needs the request, the credential, or the data to change first,
    /// so retrying it unchanged only wastes the attempt.
    pub const fn is_retryable(self) -> bool {
        matches!(self, ResultCode::Unavailable | ResultCode::Stale)
    }
}

/// The canonical surface-agnostic error reply. Every managed surface has its own
/// typed reply enum (`QueryReply`, `KvReply`, `ForkReply`, `BrowseReply`), but a
/// server that receives a command code it does not handle (a forwarded
/// `AGDX_KV_CAS` on a build without compare-and-swap, or any future additive
/// code) has no one surface to answer in: a query-shaped error reply fails to
/// decode in a client awaiting a key-value reply, and surfaces as an opaque
/// transport error instead of a clean classification. This is that fallback. A
/// server answers an unhandled or unsupported code with a `CommandError`, and a
/// client that fails to decode the surface's typed reply tries `CommandError`
/// next, turning the wrong-surface reply into a typed [`ResultCode`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommandError {
    pub code: ResultCode,
    pub message: String,
}

impl CommandError {
    /// A command error from a classified code and a human message.
    pub fn new(code: ResultCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// The reply for a command code this server does not handle.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ResultCode::Unsupported, message)
    }
}

impl From<&QueryError> for ResultCode {
    fn from(error: &QueryError) -> Self {
        match error {
            QueryError::Unsupported(_) => ResultCode::Unsupported,
            // The per-source DSL check refused the query: the caller authenticated
            // but lacks the grant for the named resource, so forbidden not 401.
            QueryError::Unauthorized(_) => ResultCode::Forbidden,
            QueryError::IndexNotFound(_) | QueryError::ForkNotFound(_) => ResultCode::NotFound,
            QueryError::Backend(_) => ResultCode::Backend,
            QueryError::Unavailable(_) => ResultCode::Unavailable,
            QueryError::TooLarge { .. } => ResultCode::TooLarge,
            QueryError::Version { .. } => ResultCode::VersionSkew,
            QueryError::Stale { .. } => ResultCode::Stale,
        }
    }
}

impl From<&KvError> for ResultCode {
    fn from(error: &KvError) -> Self {
        match error {
            KvError::Unsupported(_) => ResultCode::Unsupported,
            KvError::InvalidKey(_) => ResultCode::InvalidArgument,
            KvError::InvalidNamespace(_) => ResultCode::InvalidArgument,
            KvError::TooLarge { .. } => ResultCode::TooLarge,
            KvError::Backend(_) => ResultCode::Backend,
            KvError::Unavailable(_) => ResultCode::Unavailable,
            KvError::Version { .. } => ResultCode::VersionSkew,
            KvError::VersionConflict { .. } => ResultCode::Conflict,
            KvError::LeaseLost => ResultCode::Conflict,
            KvError::NotFound => ResultCode::NotFound,
            KvError::NotLeader => ResultCode::Unavailable,
        }
    }
}

impl From<&ForkError> for ResultCode {
    fn from(error: &ForkError) -> Self {
        match error {
            ForkError::Unsupported(_) => ResultCode::Unsupported,
            ForkError::NotFound(_) => ResultCode::NotFound,
            ForkError::InvalidFork(_) => ResultCode::InvalidArgument,
            ForkError::Conflict(_) => ResultCode::Conflict,
            ForkError::Backend(_) => ResultCode::Backend,
            ForkError::Unavailable(_) => ResultCode::Unavailable,
            ForkError::Version { .. } => ResultCode::VersionSkew,
            ForkError::NotLeader => ResultCode::Unavailable,
        }
    }
}

impl From<&GraphError> for ResultCode {
    fn from(error: &GraphError) -> Self {
        match error {
            GraphError::Unsupported(_) => ResultCode::Unsupported,
            GraphError::Unauthorized(_) => ResultCode::Forbidden,
            GraphError::InvalidName(_) => ResultCode::InvalidArgument,
            GraphError::NotFound(_) => ResultCode::NotFound,
            GraphError::TooLarge { .. } => ResultCode::TooLarge,
            GraphError::Backend(_) => ResultCode::Backend,
            GraphError::Unavailable(_) => ResultCode::Unavailable,
            GraphError::Version { .. } => ResultCode::VersionSkew,
        }
    }
}

impl From<&AgentError> for ResultCode {
    fn from(error: &AgentError) -> Self {
        match error {
            AgentError::Unsupported(_) => ResultCode::Unsupported,
            AgentError::NotFound(_) => ResultCode::NotFound,
            AgentError::Invalid(_) => ResultCode::InvalidArgument,
            AgentError::Backend(_) => ResultCode::Backend,
            AgentError::Unavailable(_) => ResultCode::Unavailable,
            AgentError::Version { .. } => ResultCode::VersionSkew,
            AgentError::NotLeader => ResultCode::Unavailable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_result_codes_when_mapped_then_should_round_trip_through_the_numeric_value() {
        for code in [
            ResultCode::Ok,
            ResultCode::Unsupported,
            ResultCode::NotFound,
            ResultCode::InvalidArgument,
            ResultCode::TooLarge,
            ResultCode::Conflict,
            ResultCode::Stale,
            ResultCode::VersionSkew,
            ResultCode::Unauthenticated,
            ResultCode::Backend,
            ResultCode::Forbidden,
            ResultCode::StepUpRequired,
            ResultCode::Unavailable,
        ] {
            assert_eq!(ResultCode::from_code(code.code()), code);
        }
        // An unknown numeric code rides through as Unrecognized.
        assert_eq!(ResultCode::from_code(900), ResultCode::Unrecognized(900));
        assert_eq!(ResultCode::Unrecognized(900).code(), 900);
    }

    #[test]
    fn given_surface_errors_when_classified_then_should_map_to_the_shared_code() {
        assert_eq!(
            ResultCode::from(&QueryError::IndexNotFound("orders".to_owned())),
            ResultCode::NotFound
        );
        assert_eq!(
            ResultCode::from(&QueryError::Stale {
                what: "orders".to_owned(),
                applied: 4,
                required: 9,
            }),
            ResultCode::Stale
        );
        assert_eq!(
            ResultCode::from(&KvError::VersionConflict { current: Some(3) }),
            ResultCode::Conflict
        );
        assert_eq!(
            ResultCode::from(&ForkError::Conflict("open".to_owned())),
            ResultCode::Conflict
        );
        assert_eq!(
            ResultCode::from(&KvError::NotLeader),
            ResultCode::Unavailable
        );
        assert_eq!(
            ResultCode::from(&ForkError::NotLeader),
            ResultCode::Unavailable
        );
        assert_eq!(
            ResultCode::from(&AgentError::NotLeader),
            ResultCode::Unavailable
        );
    }

    // A transient outcome must be distinguishable from a fault without parsing
    // the message, so a generic client can back off and retry exactly these.
    #[test]
    fn given_transient_outcomes_when_classified_then_should_be_the_only_retryable_codes() {
        for surface in [
            ResultCode::from(&QueryError::Unavailable("dropped".to_owned())),
            ResultCode::from(&KvError::Unavailable("dropped".to_owned())),
            ResultCode::from(&ForkError::Unavailable("dropped".to_owned())),
            ResultCode::from(&AgentError::Unavailable("dropped".to_owned())),
        ] {
            assert_eq!(surface, ResultCode::Unavailable);
        }
        assert_eq!(ResultCode::Unavailable.http_status(), 503);
        assert!(ResultCode::Unavailable.is_retryable());
        assert!(ResultCode::Stale.is_retryable());
        for code in [
            ResultCode::Ok,
            ResultCode::Unsupported,
            ResultCode::NotFound,
            ResultCode::InvalidArgument,
            ResultCode::TooLarge,
            ResultCode::Conflict,
            ResultCode::VersionSkew,
            ResultCode::Unauthenticated,
            ResultCode::Backend,
            ResultCode::Forbidden,
            ResultCode::StepUpRequired,
            ResultCode::Unrecognized(900),
        ] {
            assert!(
                !code.is_retryable(),
                "{code:?} must not invite a blind retry"
            );
        }
    }

    #[test]
    fn given_result_codes_when_mapped_to_http_then_should_match_the_binding_table() {
        assert_eq!(ResultCode::NotFound.http_status(), 404);
        assert_eq!(ResultCode::Unsupported.http_status(), 501);
        assert_eq!(ResultCode::TooLarge.http_status(), 413);
        assert_eq!(ResultCode::Conflict.http_status(), 409);
        assert_eq!(ResultCode::Stale.http_status(), 503);
        assert_eq!(ResultCode::Unauthenticated.http_status(), 401);
        assert_eq!(ResultCode::Forbidden.http_status(), 403);
        assert_eq!(ResultCode::StepUpRequired.http_status(), 403);
        assert_eq!(ResultCode::Backend.http_status(), 502);
        // An unrecognized code from a newer peer maps to a generic 500 rather
        // than panicking, and keeps its raw numeric.
        assert_eq!(ResultCode::Unrecognized(777).http_status(), 500);
        assert_eq!(ResultCode::Unrecognized(777).code(), 777);
    }

    #[cfg(feature = "cbor")]
    #[test]
    fn given_a_result_code_when_round_tripped_through_cbor_then_should_preserve_the_variant() {
        use crate::framing::{decode_named, encode_named};
        for code in [
            ResultCode::Ok,
            ResultCode::Conflict,
            ResultCode::Stale,
            ResultCode::Unrecognized(4242),
        ] {
            let bytes = encode_named(&code).expect("serializes");
            let back: ResultCode = decode_named(&bytes).expect("deserializes");
            assert_eq!(back, code);
        }
    }

    #[cfg(feature = "cbor")]
    #[test]
    fn given_a_command_error_when_round_tripped_then_should_preserve_code_and_message() {
        use crate::framing::{decode_named, encode_named};
        let error = CommandError::unsupported("AGDX_KV_CAS not served on this build");
        assert_eq!(error.code, ResultCode::Unsupported);
        let bytes = encode_named(&error).expect("serializes");
        let back: CommandError = decode_named(&bytes).expect("deserializes");
        assert_eq!(back, error);
    }
}
