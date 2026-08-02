use crate::types::MessageId;
use std::collections::BTreeMap;

/// A generic message read off the log: raw payload, the source `MessageId`, and
/// the user-headers decoded as strings. No agentic decoding (no `Provenance`).
/// The agent layer reconstructs that on top from the same `headers`.
#[derive(Clone, Debug)]
pub struct Message {
    /// The raw message body. Owned `Vec<u8>` so the public API never leaks the
    /// `bytes` crate.
    pub payload: Vec<u8>,
    /// Where the message sits on the log (partition + offset).
    pub id: MessageId,
    /// User headers decoded to strings (non-UTF-8 entries dropped).
    pub headers: BTreeMap<String, String>,
}
