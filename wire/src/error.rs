/// A codec, framing, or payload decode failure. Deterministic for a given
/// input, so never retryable. The SDK maps it into its own error type, and
/// ports surface it as-is.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    #[error("encode: {0}")]
    Encode(String),
    #[error("decode: {0}")]
    Decode(String),
    #[error("no payload: {0}")]
    MissingPayload(&'static str),
    /// A `[len: u32 LE][bytes]` frame violated the framing contract (over the
    /// frame cap, or a length prefix pointing past the cap).
    #[error("frame: {0}")]
    Frame(String),
}

/// Client-side validation rejected the input before any encode or round-trip
/// (missing required builder fields, empty identifiers).
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct InvalidError(pub String);

impl InvalidError {
    /// A validation failure with this message.
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}
