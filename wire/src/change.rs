use serde::{Deserialize, Serialize};

/// One materialized-view advancement on the change feed: after committing a
/// projector batch for a binding that opted in (`ProjectionBinding.notify`),
/// the plane publishes one record naming the index, the partition, the offset
/// window the batch covered, and how many rows landed. A consumer awaits the
/// record then queries, instead of sleeping and retrying: change notification
/// as records on a topic, consumed by offset, never a server-push watch (the
/// substrate is a log). v1 scope is materialized-view advancement. Key-value
/// and run-registry change records are candidate extensions of the same shape,
/// not implied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangeRecord {
    pub v: u32,
    /// The materialized index the batch advanced.
    pub index: String,
    /// The source partition the batch covered.
    pub partition_id: u32,
    /// First source offset in the committed batch.
    pub from_offset: u64,
    /// Last source offset in the committed batch (the new watermark).
    pub to_offset: u64,
    /// Rows the batch landed in the view.
    pub rows: u32,
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use super::*;
    use crate::codes::CHANGE_OP_VERSION;
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_a_change_record_when_round_tripped_then_should_decode_unchanged() {
        let record = ChangeRecord {
            v: CHANGE_OP_VERSION,
            index: "orders_v1".to_owned(),
            partition_id: 3,
            from_offset: 100,
            to_offset: 141,
            rows: 42,
        };
        let bytes = encode_named(&record).expect("encodes");
        let back: ChangeRecord = decode_named(&bytes).expect("decodes");
        assert_eq!(back, record);
    }
}
