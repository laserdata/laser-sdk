use serde::{Deserialize, Serialize};

/// A memory record as it rides the memory topic: the on-log audit of one scope's
/// changes. This is the shape the SDK's memory facade writes and the shape a
/// deployment folds into the versioned key-value read view, so both sides agree
/// on the bytes without the reader importing the SDK. Ids are the text form of
/// the SDK's memory id, kinds the snake-case kind word, so the encoding is
/// byte-identical to the SDK's own record. Runtime-free and portable, like the
/// rest of the wire contract.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum MemoryRecord {
    /// A remembered item: its id, kind word, and body.
    Item {
        id: String,
        kind: String,
        body: Vec<u8>,
    },
    /// A tombstone removing an item from recall.
    Forget { target: String },
    /// A feedback signal reweighting an item's recall rank.
    Feedback { target: String, weight: f32 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_each_variant_when_round_tripped_then_should_preserve_fields() {
        for record in [
            MemoryRecord::Item {
                id: "01KWM3K3XEP3NP5TN850J17YBP".to_owned(),
                kind: "fact".to_owned(),
                body: b"checkout is slow".to_vec(),
            },
            MemoryRecord::Forget {
                target: "01KWM3K3XEP3NP5TN850J17YBP".to_owned(),
            },
            MemoryRecord::Feedback {
                target: "01KWM3K3XEP3NP5TN850J17YBP".to_owned(),
                weight: 1.5,
            },
        ] {
            let bytes = encode_named(&record).expect("encodes");
            let back: MemoryRecord = decode_named(&bytes).expect("decodes");
            assert_eq!(back, record);
        }
    }
}
