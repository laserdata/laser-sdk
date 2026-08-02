use serde::{Deserialize, Serialize};

pub const KEY_RECORD_VERSION: u32 = 1;
pub const KEY_ID_BYTES: usize = 8;
pub const VERIFYING_KEY_BYTES: usize = 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyKind {
    Agent,
    Operator,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRecord {
    pub v: u32,
    pub principal: String,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub key_id: Vec<u8>,
    #[serde(with = "crate::encoding::bin_bytes")]
    pub verifying_key: Vec<u8>,
    pub kind: KeyKind,
    pub valid_from_micros: u64,
    pub valid_to_micros: Option<u64>,
    pub revoked: bool,
}

impl KeyRecord {
    #[must_use]
    pub fn new(
        principal: impl Into<String>,
        key_id: Vec<u8>,
        verifying_key: Vec<u8>,
        kind: KeyKind,
    ) -> Self {
        Self {
            v: KEY_RECORD_VERSION,
            principal: principal.into(),
            key_id,
            verifying_key,
            kind,
            valid_from_micros: 0,
            valid_to_micros: None,
            revoked: false,
        }
    }

    pub fn validate(&self) -> Result<(), &'static str> {
        if self.v != KEY_RECORD_VERSION {
            return Err("unsupported key record version");
        }
        if self.principal.is_empty() {
            return Err("key principal must not be empty");
        }
        if self.key_id.len() != KEY_ID_BYTES {
            return Err("Ed25519 key id must be 8 bytes");
        }
        if self.verifying_key.len() != VERIFYING_KEY_BYTES {
            return Err("Ed25519 public key must be 32 bytes");
        }
        if self
            .valid_to_micros
            .is_some_and(|end| end <= self.valid_from_micros)
        {
            return Err("key validity end must be after its start");
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "cbor"))]
mod tests {
    use super::*;
    use crate::framing::{decode_named, encode_named};

    #[test]
    fn given_a_key_record_when_round_tripped_then_should_preserve_its_lifecycle() {
        let record = KeyRecord {
            v: KEY_RECORD_VERSION,
            principal: "operator-1".to_owned(),
            key_id: vec![3; KEY_ID_BYTES],
            verifying_key: vec![7; VERIFYING_KEY_BYTES],
            kind: KeyKind::Operator,
            valid_from_micros: 100,
            valid_to_micros: Some(200),
            revoked: true,
        };

        let encoded = encode_named(&record).expect("key record encodes");
        let decoded: KeyRecord = decode_named(&encoded).expect("key record decodes");

        assert_eq!(decoded, record);
        assert!(decoded.validate().is_ok());
    }
}
