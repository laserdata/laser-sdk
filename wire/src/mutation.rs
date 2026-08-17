use serde::{Deserialize, Serialize};

pub const MANAGED_REQUEST_VERSION: u32 = 1;

/// Position of one applied mutation on the KV mutation topic: the fold
/// coordinate a barriered read waits for. A lease grant or renewal returns the
/// position at which the answering fold applied it, and a
/// [`crate::kv::KvGet`] carrying it as `min_position` must not be answered by
/// a fold that has not applied that position yet. Positions compare only
/// within one `topic_generation`: a generation mismatch is fail-closed
/// ([`crate::kv::KvError::Stale`]), never an implicit reset.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationPosition {
    pub topic_generation: u64,
    pub partition: u32,
    pub offset: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedRequestEnvelope {
    pub v: u32,
    pub operation_id: u128,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub payload: Vec<u8>,
}

impl ManagedRequestEnvelope {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.v != MANAGED_REQUEST_VERSION {
            return Err("unsupported managed request version");
        }
        if self.operation_id == 0 {
            return Err("managed request operation id must not be zero");
        }
        Ok(())
    }
}

/// One managed command stored on a mutation topic. `payload` is the canonical
/// typed request encoded with the normal wire framing for `command_code`, so
/// append and fold share one request schema instead of maintaining a second
/// mutation representation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MutationCommandEnvelope {
    pub v: u32,
    pub operation_id: u128,
    pub timestamp_micros: u64,
    pub command_code: u32,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub payload: Vec<u8>,
}

impl MutationCommandEnvelope {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.v == 0 {
            return Err("mutation command version must not be zero");
        }
        if self.operation_id == 0 {
            return Err("mutation command operation id must not be zero");
        }
        Ok(())
    }
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
            operation_id: 42,
            timestamp_micros: 1_700_000_000_000_000,
            command_code: AGDX_KV_SET_CODE,
            payload: vec![0, 1, 2, 255],
        };
        let bytes = encode_named(&envelope).expect("encodes");
        let back: MutationCommandEnvelope = decode_named(&bytes).expect("decodes");
        back.validate().expect("the envelope is valid");
        assert_eq!(back.v, KV_OP_VERSION);
        assert_eq!(back.operation_id, 42);
        assert_eq!(back.timestamp_micros, 1_700_000_000_000_000);
        assert_eq!(back.command_code, AGDX_KV_SET_CODE);
        assert_eq!(back.payload, vec![0, 1, 2, 255]);
    }

    #[test]
    fn given_a_managed_request_when_round_tripped_then_should_preserve_operation_identity() {
        let request = ManagedRequestEnvelope {
            v: MANAGED_REQUEST_VERSION,
            operation_id: u128::MAX,
            payload: vec![4, 3, 2, 1],
        };

        let bytes = encode_named(&request).expect("managed request encodes");
        let back: ManagedRequestEnvelope = decode_named(&bytes).expect("managed request decodes");

        assert_eq!(back, request);
        back.validate().expect("the request is valid");
    }

    #[test]
    fn given_a_zero_operation_identity_when_validated_then_should_reject_it() {
        let request = ManagedRequestEnvelope {
            v: MANAGED_REQUEST_VERSION,
            operation_id: 0,
            payload: Vec::new(),
        };
        let command = MutationCommandEnvelope {
            v: KV_OP_VERSION,
            operation_id: 0,
            timestamp_micros: 1,
            command_code: AGDX_KV_SET_CODE,
            payload: Vec::new(),
        };

        assert!(request.validate().is_err());
        assert!(command.validate().is_err());
    }
}
