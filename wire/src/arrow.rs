use crate::error::InvalidError;
use crate::schema::SchemaFingerprint;
use crate::validate::Validate;
use serde::{Deserialize, Serialize};

pub const ARROW_IPC_CONTRACT_VERSION: u32 = 1;
pub const ARROW_IPC_MEDIA_TYPE: &str = "application/vnd.apache.arrow.stream";
pub const MAX_ARROW_IPC_MESSAGE_BYTES: u64 = 8 * 1024 * 1024;
pub const MAX_ARROW_IPC_FIELDS: u32 = 4_096;
pub const MAX_ARROW_IPC_BATCHES: u32 = 64;
pub const MAX_ARROW_IPC_ROWS: u64 = 1_000_000;
pub const MAX_ARROW_IPC_DICTIONARIES: u32 = 4_096;
pub const MAX_ARROW_DECIMAL_BITS: u16 = 128;

/// Validated facts about one self-contained Arrow IPC stream carried by one
/// Iggy message. The raw IPC stream remains the message body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrowIpcMessageMetadata {
    pub contract_version: u32,
    pub schema_fingerprint: SchemaFingerprint,
    pub encoded_bytes: u64,
    pub field_count: u32,
    pub record_batch_count: u32,
    pub row_count: u64,
    pub dictionary_count: u32,
}

impl Validate for ArrowIpcMessageMetadata {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.contract_version != ARROW_IPC_CONTRACT_VERSION {
            return Err(InvalidError::new(format!(
                "Arrow IPC contract version must be {ARROW_IPC_CONTRACT_VERSION}, got {}",
                self.contract_version
            )));
        }
        self.schema_fingerprint.validate()?;
        if self.encoded_bytes == 0 || self.encoded_bytes > MAX_ARROW_IPC_MESSAGE_BYTES {
            return Err(InvalidError::new(format!(
                "Arrow IPC message bytes must be in 1..={MAX_ARROW_IPC_MESSAGE_BYTES}"
            )));
        }
        if self.field_count == 0 || self.field_count > MAX_ARROW_IPC_FIELDS {
            return Err(InvalidError::new(format!(
                "Arrow IPC field count must be in 1..={MAX_ARROW_IPC_FIELDS}"
            )));
        }
        if self.record_batch_count == 0 || self.record_batch_count > MAX_ARROW_IPC_BATCHES {
            return Err(InvalidError::new(format!(
                "Arrow IPC batch count must be in 1..={MAX_ARROW_IPC_BATCHES}"
            )));
        }
        if self.row_count > MAX_ARROW_IPC_ROWS {
            return Err(InvalidError::new(format!(
                "Arrow IPC row count exceeds cap {MAX_ARROW_IPC_ROWS}"
            )));
        }
        if self.dictionary_count > MAX_ARROW_IPC_DICTIONARIES {
            return Err(InvalidError::new(format!(
                "Arrow IPC dictionary count exceeds cap {MAX_ARROW_IPC_DICTIONARIES}"
            )));
        }
        Ok(())
    }
}

/// Frozen producer-side Arrow input policy. One message contains one complete
/// stream with its schema, dictionaries, and record batches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArrowIpcPolicy {
    pub contract_version: u32,
    pub stream_format_only: bool,
    pub self_contained: bool,
    pub dictionary_deltas: bool,
    pub replacement_dictionaries: bool,
    pub timestamp_unit: ArrowTimestampUnit,
    pub max_decimal_bits: u16,
    pub unions: bool,
    pub extension_types: bool,
}

impl Default for ArrowIpcPolicy {
    fn default() -> Self {
        Self {
            contract_version: ARROW_IPC_CONTRACT_VERSION,
            stream_format_only: true,
            self_contained: true,
            dictionary_deltas: false,
            replacement_dictionaries: false,
            timestamp_unit: ArrowTimestampUnit::Microsecond,
            max_decimal_bits: MAX_ARROW_DECIMAL_BITS,
            unions: false,
            extension_types: false,
        }
    }
}

impl Validate for ArrowIpcPolicy {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.contract_version != ARROW_IPC_CONTRACT_VERSION {
            return Err(InvalidError::new(format!(
                "Arrow IPC policy version must be {ARROW_IPC_CONTRACT_VERSION}, got {}",
                self.contract_version
            )));
        }
        if !self.stream_format_only || !self.self_contained {
            return Err(InvalidError::new(
                "Arrow IPC input must be one self-contained stream per message",
            ));
        }
        if self.dictionary_deltas || self.replacement_dictionaries {
            return Err(InvalidError::new(
                "Arrow IPC dictionary deltas and replacements are not supported",
            ));
        }
        if self.max_decimal_bits != MAX_ARROW_DECIMAL_BITS {
            return Err(InvalidError::new(format!(
                "Arrow IPC decimal width must be {MAX_ARROW_DECIMAL_BITS} bits"
            )));
        }
        if self.unions || self.extension_types {
            return Err(InvalidError::new(
                "Arrow IPC unions and extension types are not supported",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ArrowTimestampUnit {
    Microsecond,
}

/// Stable reason returned when an Arrow stream cannot enter the logical schema gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ArrowIpcRejectionCode {
    FileFormat,
    MissingSchema,
    MissingDictionary,
    DictionaryDelta,
    DictionaryReplacement,
    Union,
    ExtensionType,
    TimestampUnit,
    DecimalWidth,
    SchemaFingerprint,
    FieldLimit,
    BatchLimit,
    RowLimit,
    ByteLimit,
    MalformedStream,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_the_frozen_policy_when_validated_then_should_accept_only_the_supported_arrow_subset() {
        ArrowIpcPolicy::default()
            .validate()
            .expect("default policy is valid");
        let policy = ArrowIpcPolicy {
            dictionary_deltas: true,
            ..ArrowIpcPolicy::default()
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn given_message_metadata_when_validated_then_should_enforce_every_admission_bound() {
        let metadata = ArrowIpcMessageMetadata {
            contract_version: ARROW_IPC_CONTRACT_VERSION,
            schema_fingerprint: SchemaFingerprint::new([7; 32]),
            encoded_bytes: 1024,
            field_count: 4,
            record_batch_count: 1,
            row_count: 128,
            dictionary_count: 0,
        };
        metadata.validate().expect("metadata is valid");
        let mut oversized = metadata.clone();
        oversized.encoded_bytes = MAX_ARROW_IPC_MESSAGE_BYTES + 1;
        assert!(oversized.validate().is_err());
    }
}
