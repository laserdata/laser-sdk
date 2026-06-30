// The unified result-code space. Each managed surface keeps its own typed error
// for the detail a caller needs, but every one of those errors also projects
// onto one logical `ResultCode` here, so a generic client, the HTTP status
// mapper, and a cross-language port all dispatch on one small dictionary
// instead of parsing per-surface strings. The codes are a pinned cross-repo
// contract, and an unknown code from a newer peer rides through as
// `Unrecognized` rather than failing, the same forward-compat shape the growable
// u8 dictionaries use.

use crate::fork::ForkError;
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
    /// The credential is missing or invalid.
    Unauthorized,
    /// The managed backend failed or was unreachable.
    Backend,
    /// A code from a newer peer this build does not name. Decodes and re-encodes
    /// byte-for-byte so an old build relays it rather than failing. Only a value
    /// outside the named range (10 and up) should ever appear here: `from_code`
    /// never produces `Unrecognized` for `0..=9`, which map to the named
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
            ResultCode::Unauthorized => 8,
            ResultCode::Backend => 9,
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
            8 => ResultCode::Unauthorized,
            9 => ResultCode::Backend,
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
            ResultCode::Unauthorized => 401,
            ResultCode::Backend => 502,
            ResultCode::Unrecognized(_) => 500,
        }
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
            QueryError::IndexNotFound(_) | QueryError::ForkNotFound(_) => ResultCode::NotFound,
            QueryError::Backend(_) => ResultCode::Backend,
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
            KvError::TooLarge { .. } => ResultCode::TooLarge,
            KvError::Backend(_) => ResultCode::Backend,
            KvError::Version { .. } => ResultCode::VersionSkew,
            KvError::VersionConflict { .. } => ResultCode::Conflict,
            KvError::LeaseLost => ResultCode::Conflict,
            KvError::NotFound => ResultCode::NotFound,
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
            ForkError::Version { .. } => ResultCode::VersionSkew,
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
            ResultCode::Unauthorized,
            ResultCode::Backend,
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
    }

    #[test]
    fn given_result_codes_when_mapped_to_http_then_should_match_the_binding_table() {
        assert_eq!(ResultCode::NotFound.http_status(), 404);
        assert_eq!(ResultCode::Unsupported.http_status(), 501);
        assert_eq!(ResultCode::TooLarge.http_status(), 413);
        assert_eq!(ResultCode::Conflict.http_status(), 409);
        assert_eq!(ResultCode::Stale.http_status(), 503);
        assert_eq!(ResultCode::Unauthorized.http_status(), 401);
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
