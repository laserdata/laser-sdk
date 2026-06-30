use crate::error::LaserError;
use laser_wire::framing::{decode_named, encode_named};
pub use laser_wire::snapshot::FoldSnapshot;

/// Encode a fold snapshot to its canonical bytes, for storage as a key-value
/// value or a snapshot-topic body. The inverse of [`decode`].
pub fn encode(snapshot: &FoldSnapshot) -> Result<Vec<u8>, LaserError> {
    encode_named(snapshot).map_err(|error| LaserError::Codec(format!("encode snapshot: {error}")))
}

/// Decode a fold snapshot from stored bytes. The inverse of [`encode`].
pub fn decode(bytes: &[u8]) -> Result<FoldSnapshot, LaserError> {
    decode_named(bytes).map_err(LaserError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use laser_wire::agent::ConversationId;
    use std::collections::BTreeMap;

    #[test]
    fn given_a_snapshot_when_round_tripped_through_bytes_then_should_be_unchanged() {
        let snapshot = FoldSnapshot {
            conversation: ConversationId::from_u128(7),
            as_of: BTreeMap::from([(0, 41), (1, 9)]),
            state: br#"{"folded":true}"#.to_vec(),
        };
        assert_eq!(
            decode(&encode(&snapshot).expect("encodes")).expect("decodes"),
            snapshot
        );
    }
}
