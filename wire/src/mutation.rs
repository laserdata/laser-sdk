use serde::{Deserialize, Serialize};

/// One managed command stored on a mutation topic. `payload` is the canonical
/// typed request encoded with the normal wire framing for `command_code`, so
/// append and fold share one request schema instead of maintaining a second
/// mutation representation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MutationCommandEnvelope {
    pub v: u32,
    pub timestamp_micros: u64,
    pub command_code: u32,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub payload: Vec<u8>,
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use super::*;
    use crate::codes::{AGDX_KV_SET_CODE, KV_OP_VERSION};
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_a_command_envelope_when_round_tripped_then_should_preserve_request_bytes() {
        let envelope = MutationCommandEnvelope {
            v: KV_OP_VERSION,
            timestamp_micros: 1_700_000_000_000_000,
            command_code: AGDX_KV_SET_CODE,
            payload: vec![0, 1, 2, 255],
        };
        let bytes = encode_named(&envelope).expect("encodes");
        let back: MutationCommandEnvelope = decode_named(&bytes).expect("decodes");
        assert_eq!(back.v, KV_OP_VERSION);
        assert_eq!(back.timestamp_micros, 1_700_000_000_000_000);
        assert_eq!(back.command_code, AGDX_KV_SET_CODE);
        assert_eq!(back.payload, vec![0, 1, 2, 255]);
    }
}
