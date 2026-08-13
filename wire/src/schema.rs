use crate::error::InvalidError;
use crate::limits::MAX_VALUE_BYTES;
use crate::validate::Validate;
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt;

pub const MAX_LOGICAL_SCHEMA_DEPTH: usize = 16;
pub const MAX_LOGICAL_SCHEMA_FIELDS: usize = 4_096;
pub const MAX_LOGICAL_SCHEMA_BYTES: usize = 1_048_576;
pub const MAX_FIELD_NAME_BYTES: usize = 255;
pub const MAX_FIELD_DOC_BYTES: usize = 4_096;
pub const MAX_FIXED_BYTES: u32 = 1_048_576;
pub const MAX_DECIMAL_PRECISION: u8 = 38;
pub const MICROS_PER_DAY: i64 = 86_400_000_000;
pub const PROVENANCE_FIELD_ID_START: u32 = 2_000_000_000;

pub const PROVENANCE_FIELDS: &[(u32, &str)] = &[
    (2_000_000_001, "__laser_cluster_incarnation"),
    (2_000_000_002, "__laser_source_incarnation"),
    (2_000_000_003, "__laser_stream_id"),
    (2_000_000_004, "__laser_topic_id"),
    (2_000_000_005, "__laser_partition_id"),
    (2_000_000_006, "__laser_offset"),
    (2_000_000_007, "__laser_row_ordinal"),
    (2_000_000_008, "__laser_projection_id"),
    (2_000_000_009, "__laser_projection_version"),
    (2_000_000_010, "__laser_destination_id"),
    (2_000_000_011, "__laser_destination_generation"),
    (2_000_000_012, "__laser_original_payload"),
    (2_000_000_013, "__laser_original_content_type"),
    (2_000_000_014, "__laser_original_schema_id"),
];

pub const ORIGINAL_PAYLOAD_FIELD_ID: u32 = 2_000_000_012;
pub const ORIGINAL_PAYLOAD_FIELD_NAME: &str = "__laser_original_payload";
pub const ORIGINAL_CONTENT_TYPE_FIELD_ID: u32 = 2_000_000_013;
pub const ORIGINAL_CONTENT_TYPE_FIELD_NAME: &str = "__laser_original_content_type";
pub const ORIGINAL_SCHEMA_ID_FIELD_ID: u32 = 2_000_000_014;
pub const ORIGINAL_SCHEMA_ID_FIELD_NAME: &str = "__laser_original_schema_id";
pub const PARTITION_ID_FIELD_NAME: &str = "__laser_partition_id";
pub const OFFSET_FIELD_NAME: &str = "__laser_offset";

/// Immutable identity of one logical schema family.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogicalSchemaId(u128);

impl LogicalSchemaId {
    pub const fn from_u128(value: u128) -> Self {
        Self(value)
    }

    pub const fn as_u128(self) -> u128 {
        self.0
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0.to_be_bytes()
    }

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(u128::from_be_bytes(bytes))
    }
}

impl Serialize for LogicalSchemaId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.to_bytes())
    }
}

impl<'de> Deserialize<'de> for LogicalSchemaId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct LogicalSchemaIdVisitor;

        impl<'de> Visitor<'de> for LogicalSchemaIdVisitor {
            type Value = LogicalSchemaId;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("16 big-endian logical schema id bytes")
            }

            fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
                let bytes = bytes
                    .try_into()
                    .map_err(|_| E::invalid_length(bytes.len(), &self))?;
                Ok(LogicalSchemaId::from_bytes(bytes))
            }

            fn visit_byte_buf<E: de::Error>(self, bytes: Vec<u8>) -> Result<Self::Value, E> {
                self.visit_bytes(&bytes)
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Self::Value, A::Error> {
                let mut bytes = [0_u8; 16];
                for (index, byte) in bytes.iter_mut().enumerate() {
                    *byte = sequence
                        .next_element()?
                        .ok_or_else(|| de::Error::invalid_length(index, &self))?;
                }
                if sequence.next_element::<u8>()?.is_some() {
                    return Err(de::Error::invalid_length(17, &self));
                }
                Ok(LogicalSchemaId::from_bytes(bytes))
            }
        }

        deserializer.deserialize_bytes(LogicalSchemaIdVisitor)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaFingerprint(#[serde(with = "crate::encoding::bin_bytes")] pub Vec<u8>);

impl SchemaFingerprint {
    pub const BYTES: usize = 32;

    pub fn new(bytes: [u8; Self::BYTES]) -> Self {
        Self(bytes.to_vec())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Digest32(#[serde(with = "crate::encoding::bin_bytes")] pub Vec<u8>);

impl Digest32 {
    pub const BYTES: usize = 32;

    pub fn new(bytes: [u8; Self::BYTES]) -> Self {
        Self(bytes.to_vec())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl Validate for Digest32 {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.0.len() != Self::BYTES {
            return Err(InvalidError::new(format!(
                "digest must be {} bytes, got {}",
                Self::BYTES,
                self.0.len()
            )));
        }
        Ok(())
    }
}

impl Validate for SchemaFingerprint {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.0.len() != Self::BYTES {
            return Err(InvalidError::new(format!(
                "schema fingerprint must be {} bytes, got {}",
                Self::BYTES,
                self.0.len()
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalSchemaRef {
    pub id: LogicalSchemaId,
    pub version: u32,
    pub fingerprint: SchemaFingerprint,
}

impl Validate for LogicalSchemaRef {
    fn validate(&self) -> Result<(), InvalidError> {
        if self.id.as_u128() == 0 {
            return Err(InvalidError::new("logical schema id must be nonzero"));
        }
        if self.version == 0 {
            return Err(InvalidError::new("logical schema version must be nonzero"));
        }
        self.fingerprint.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalSchema {
    pub schema: LogicalSchemaRef,
    pub fields: Vec<LogicalField>,
}

impl Validate for LogicalSchema {
    fn validate(&self) -> Result<(), InvalidError> {
        self.validate_shape()?;
        let computed = self.compute_fingerprint()?;
        if self.schema.fingerprint != computed {
            return Err(InvalidError::new(
                "logical schema fingerprint does not match its canonical bytes",
            ));
        }
        Ok(())
    }
}

impl LogicalSchema {
    pub fn new(
        id: LogicalSchemaId,
        version: u32,
        fields: Vec<LogicalField>,
    ) -> Result<Self, InvalidError> {
        let mut schema = Self {
            schema: LogicalSchemaRef {
                id,
                version,
                fingerprint: SchemaFingerprint(Vec::new()),
            },
            fields,
        };
        schema.validate_shape()?;
        schema.schema.fingerprint = schema.compute_fingerprint()?;
        Ok(schema)
    }

    pub fn canonical_fingerprint_bytes(&self) -> Result<Vec<u8>, InvalidError> {
        self.validate_shape()?;
        let mut bytes = b"AGDX-SCHEMA-V1\0".to_vec();
        bytes.extend_from_slice(&self.schema.id.to_bytes());
        bytes.extend_from_slice(&self.schema.version.to_be_bytes());
        encode_fields(&mut bytes, &self.fields)?;
        Ok(bytes)
    }

    pub fn compute_fingerprint(&self) -> Result<SchemaFingerprint, InvalidError> {
        let digest: [u8; SchemaFingerprint::BYTES] =
            Sha256::digest(self.canonical_fingerprint_bytes()?).into();
        Ok(SchemaFingerprint::new(digest))
    }

    fn validate_shape(&self) -> Result<(), InvalidError> {
        if self.schema.id.as_u128() == 0 {
            return Err(InvalidError::new("logical schema id must be nonzero"));
        }
        if self.schema.version == 0 {
            return Err(InvalidError::new("logical schema version must be nonzero"));
        }
        if self.fields.is_empty() {
            return Err(InvalidError::new("logical schema must contain a field"));
        }

        let mut ids = BTreeSet::new();
        let mut field_count = 0usize;
        let mut encoded_size = 0usize;
        validate_struct_fields(
            &self.fields,
            1,
            &mut ids,
            &mut field_count,
            &mut encoded_size,
            false,
        )?;
        Ok(())
    }
}

pub(crate) fn validate_result_fields(fields: &[LogicalField]) -> Result<(), InvalidError> {
    if fields.is_empty() {
        return Err(InvalidError::new("query result schema must not be empty"));
    }
    let mut ids = BTreeSet::new();
    let mut field_count = 0usize;
    let mut encoded_size = 0usize;
    validate_struct_fields(
        fields,
        1,
        &mut ids,
        &mut field_count,
        &mut encoded_size,
        true,
    )
}

fn encode_fields(bytes: &mut Vec<u8>, fields: &[LogicalField]) -> Result<(), InvalidError> {
    let count = u32::try_from(fields.len())
        .map_err(|_| InvalidError::new("logical schema field count does not fit u32"))?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for field in fields {
        bytes.extend_from_slice(&field.id.to_be_bytes());
        bytes.push(u8::from(field.required));
        encode_text(bytes, &field.name)?;
        match &field.doc {
            Some(doc) => {
                bytes.push(1);
                encode_text(bytes, doc)?;
            }
            None => bytes.push(0),
        }
        encode_logical_type(bytes, &field.field_type)?;
    }
    Ok(())
}

fn encode_text(bytes: &mut Vec<u8>, value: &str) -> Result<(), InvalidError> {
    let length = u32::try_from(value.len())
        .map_err(|_| InvalidError::new("logical schema text length does not fit u32"))?;
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn encode_logical_type(
    bytes: &mut Vec<u8>,
    logical_type: &LogicalType,
) -> Result<(), InvalidError> {
    match logical_type {
        LogicalType::Boolean => bytes.push(0),
        LogicalType::Int => bytes.push(1),
        LogicalType::Long => bytes.push(2),
        LogicalType::Float => bytes.push(3),
        LogicalType::Double => bytes.push(4),
        LogicalType::Decimal { precision, scale } => {
            bytes.extend_from_slice(&[5, *precision, *scale]);
        }
        LogicalType::Date => bytes.push(6),
        LogicalType::TimeMicros => bytes.push(7),
        LogicalType::TimestampMicros => bytes.push(8),
        LogicalType::TimestampTzMicros => bytes.push(9),
        LogicalType::String => bytes.push(10),
        LogicalType::Uuid => bytes.push(11),
        LogicalType::Fixed { length } => {
            bytes.push(12);
            bytes.extend_from_slice(&length.to_be_bytes());
        }
        LogicalType::Binary => bytes.push(13),
        LogicalType::Struct { fields } => {
            bytes.push(14);
            encode_fields(bytes, fields)?;
        }
        LogicalType::List {
            element_id,
            element_required,
            element,
        } => {
            bytes.push(15);
            bytes.extend_from_slice(&element_id.to_be_bytes());
            bytes.push(u8::from(*element_required));
            encode_logical_type(bytes, element)?;
        }
        LogicalType::Map {
            key_id,
            key,
            value_id,
            value_required,
            value,
        } => {
            bytes.push(16);
            bytes.extend_from_slice(&key_id.to_be_bytes());
            encode_logical_type(bytes, key)?;
            bytes.extend_from_slice(&value_id.to_be_bytes());
            bytes.push(u8::from(*value_required));
            encode_logical_type(bytes, value)?;
        }
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogicalField {
    pub id: u32,
    pub name: String,
    pub required: bool,
    pub field_type: LogicalType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum LogicalType {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal {
        precision: u8,
        scale: u8,
    },
    Date,
    TimeMicros,
    TimestampMicros,
    TimestampTzMicros,
    String,
    Uuid,
    Fixed {
        length: u32,
    },
    Binary,
    Struct {
        fields: Vec<LogicalField>,
    },
    List {
        element_id: u32,
        element_required: bool,
        element: Box<LogicalType>,
    },
    Map {
        key_id: u32,
        key: Box<LogicalType>,
        value_id: u32,
        value_required: bool,
        value: Box<LogicalType>,
    },
}

/// Non-parameterized logical type identity used in capability descriptors.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum LogicalTypeKind {
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Decimal,
    Date,
    TimeMicros,
    TimestampMicros,
    TimestampTzMicros,
    String,
    Uuid,
    Fixed,
    Binary,
    Struct,
    List,
    Map,
}

impl LogicalType {
    pub const fn kind(&self) -> LogicalTypeKind {
        match self {
            Self::Boolean => LogicalTypeKind::Boolean,
            Self::Int => LogicalTypeKind::Int,
            Self::Long => LogicalTypeKind::Long,
            Self::Float => LogicalTypeKind::Float,
            Self::Double => LogicalTypeKind::Double,
            Self::Decimal { .. } => LogicalTypeKind::Decimal,
            Self::Date => LogicalTypeKind::Date,
            Self::TimeMicros => LogicalTypeKind::TimeMicros,
            Self::TimestampMicros => LogicalTypeKind::TimestampMicros,
            Self::TimestampTzMicros => LogicalTypeKind::TimestampTzMicros,
            Self::String => LogicalTypeKind::String,
            Self::Uuid => LogicalTypeKind::Uuid,
            Self::Fixed { .. } => LogicalTypeKind::Fixed,
            Self::Binary => LogicalTypeKind::Binary,
            Self::Struct { .. } => LogicalTypeKind::Struct,
            Self::List { .. } => LogicalTypeKind::List,
            Self::Map { .. } => LogicalTypeKind::Map,
        }
    }
}

impl LogicalType {
    pub fn accepts_map_key(&self) -> bool {
        matches!(
            self,
            Self::Boolean
                | Self::Int
                | Self::Long
                | Self::Decimal { .. }
                | Self::Date
                | Self::TimeMicros
                | Self::TimestampMicros
                | Self::TimestampTzMicros
                | Self::String
                | Self::Uuid
                | Self::Fixed { .. }
                | Self::Binary
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecimalValue {
    #[serde(with = "crate::encoding::bin_bytes")]
    pub unscaled: Vec<u8>,
    pub precision: u8,
    pub scale: u8,
}

impl DecimalValue {
    pub fn validate_canonical(&self) -> Result<(), InvalidError> {
        validate_decimal(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UuidValue(#[serde(with = "crate::encoding::bin_bytes")] pub Vec<u8>);

impl UuidValue {
    pub const BYTES: usize = 16;

    pub fn new(bytes: [u8; Self::BYTES]) -> Self {
        Self(bytes.to_vec())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for UuidValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.len() != Self::BYTES {
            return Err(fmt::Error);
        }
        for (index, byte) in self.0.iter().enumerate() {
            if matches!(index, 4 | 6 | 8 | 10) {
                formatter.write_str("-")?;
            }
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::str::FromStr for UuidValue {
    type Err = InvalidError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != 36
            || value
                .bytes()
                .enumerate()
                .any(|(index, byte)| matches!(index, 8 | 13 | 18 | 23) != (byte == b'-'))
        {
            return Err(InvalidError::new(
                "UUID must use the lowercase canonical 8-4-4-4-12 form",
            ));
        }
        if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err(InvalidError::new(
                "UUID hexadecimal digits must be lowercase",
            ));
        }

        let mut bytes = [0u8; Self::BYTES];
        let mut output = 0usize;
        let mut nibbles = value.bytes().filter(|byte| *byte != b'-');
        while output < bytes.len() {
            let high = decode_hex(nibbles.next().expect("UUID shape checked"))?;
            let low = decode_hex(nibbles.next().expect("UUID shape checked"))?;
            bytes[output] = (high << 4) | low;
            output += 1;
        }
        Ok(Self::new(bytes))
    }
}

fn decode_hex(byte: u8) -> Result<u8, InvalidError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(InvalidError::new("UUID contains a non-hexadecimal byte")),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BinaryValue(#[serde(with = "crate::encoding::bin_bytes")] pub Vec<u8>);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
#[non_exhaustive]
pub enum TypedValue {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Decimal(DecimalValue),
    Date(i32),
    TimeMicros(i64),
    TimestampMicros(i64),
    TimestampTzMicros(i64),
    String(String),
    Uuid(UuidValue),
    Fixed(BinaryValue),
    Binary(BinaryValue),
    Struct(Vec<FieldValue>),
    List(Vec<TypedValue>),
    Map(Vec<MapEntry>),
}

impl From<&str> for TypedValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for TypedValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&String> for TypedValue {
    fn from(value: &String) -> Self {
        Self::String(value.clone())
    }
}

impl From<i32> for TypedValue {
    fn from(value: i32) -> Self {
        Self::Int(value)
    }
}

impl From<u32> for TypedValue {
    fn from(value: u32) -> Self {
        Self::Long(i64::from(value))
    }
}

impl From<i64> for TypedValue {
    fn from(value: i64) -> Self {
        Self::Long(value)
    }
}

impl From<f32> for TypedValue {
    fn from(value: f32) -> Self {
        Self::Float(value)
    }
}

impl From<f64> for TypedValue {
    fn from(value: f64) -> Self {
        Self::Double(value)
    }
}

impl From<bool> for TypedValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
    }
}

impl<T: Into<TypedValue>> From<Vec<T>> for TypedValue {
    fn from(values: Vec<T>) -> Self {
        Self::List(values.into_iter().map(Into::into).collect())
    }
}

impl TypedValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Int(value) => u64::try_from(*value).ok(),
            Self::Long(value) => u64::try_from(*value).ok(),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(i64::from(*value)),
            Self::Long(value) => Some(*value),
            _ => None,
        }
    }

    pub fn diagnostic_text(&self) -> String {
        match self {
            Self::Null => "null".to_owned(),
            Self::Boolean(value) => value.to_string(),
            Self::Int(value) => value.to_string(),
            Self::Long(value) => value.to_string(),
            Self::Float(value) => value.to_string(),
            Self::Double(value) => value.to_string(),
            Self::Date(value) => value.to_string(),
            Self::TimeMicros(value) => value.to_string(),
            Self::TimestampMicros(value) => value.to_string(),
            Self::TimestampTzMicros(value) => value.to_string(),
            Self::String(value) => value.clone(),
            Self::Uuid(value) => value.to_string(),
            Self::Decimal(value) => {
                format!("0x{} scale {}", encode_hex(&value.unscaled), value.scale)
            }
            Self::Fixed(value) | Self::Binary(value) => format!("0x{}", encode_hex(&value.0)),
            Self::Struct(_) | Self::List(_) | Self::Map(_) => {
                serde_json::to_string(self).unwrap_or_else(|_| "<invalid typed value>".to_owned())
            }
        }
    }

    pub fn validate_canonical(&self) -> Result<(), InvalidError> {
        validate_typed_value(self, 1)
    }

    pub fn validate_against(
        &self,
        logical_type: &LogicalType,
        required: bool,
    ) -> Result<(), InvalidError> {
        if matches!(self, Self::Null) {
            return if required {
                Err(InvalidError::new("required value is null"))
            } else {
                Ok(())
            };
        }

        self.validate_canonical()?;

        match (self, logical_type) {
            (Self::Boolean(_), LogicalType::Boolean)
            | (Self::Int(_), LogicalType::Int)
            | (Self::Long(_), LogicalType::Long)
            | (Self::Date(_), LogicalType::Date)
            | (Self::TimestampMicros(_), LogicalType::TimestampMicros)
            | (Self::TimestampTzMicros(_), LogicalType::TimestampTzMicros)
            | (Self::String(_), LogicalType::String)
            | (Self::Binary(_), LogicalType::Binary) => Ok(()),
            (Self::Float(value), LogicalType::Float) => validate_float(*value as f64),
            (Self::Double(value), LogicalType::Double) => validate_float(*value),
            (Self::Decimal(value), LogicalType::Decimal { precision, scale }) => {
                value.validate_canonical()?;
                if value.precision != *precision || value.scale != *scale {
                    return Err(InvalidError::new(format!(
                        "decimal type mismatch: value is ({}, {}), schema is ({precision}, {scale})",
                        value.precision, value.scale
                    )));
                }
                Ok(())
            }
            (Self::TimeMicros(value), LogicalType::TimeMicros) => {
                if !(0..MICROS_PER_DAY).contains(value) {
                    return Err(InvalidError::new(format!(
                        "time value {value} is outside one day in microseconds"
                    )));
                }
                Ok(())
            }
            (Self::Uuid(value), LogicalType::Uuid) => {
                if value.0.len() != UuidValue::BYTES {
                    return Err(InvalidError::new(format!(
                        "UUID must be {} bytes, got {}",
                        UuidValue::BYTES,
                        value.0.len()
                    )));
                }
                Ok(())
            }
            (Self::Fixed(value), LogicalType::Fixed { length }) => {
                if value.0.len() != *length as usize {
                    return Err(InvalidError::new(format!(
                        "fixed value is {} bytes, expected {length}",
                        value.0.len()
                    )));
                }
                Ok(())
            }
            (Self::Struct(values), LogicalType::Struct { fields }) => {
                validate_struct_value(values, fields)
            }
            (
                Self::List(values),
                LogicalType::List {
                    element_required,
                    element,
                    ..
                },
            ) => {
                for value in values {
                    value.validate_against(element, *element_required)?;
                }
                Ok(())
            }
            (
                Self::Map(entries),
                LogicalType::Map {
                    key,
                    value,
                    value_required,
                    ..
                },
            ) => validate_map(entries, key, value, *value_required),
            _ => Err(InvalidError::new("typed value does not match logical type")),
        }
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FieldValue {
    pub field_id: u32,
    pub value: TypedValue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MapEntry {
    pub key: TypedValue,
    pub value: TypedValue,
}

impl Validate for TypedValue {
    fn validate(&self) -> Result<(), InvalidError> {
        self.validate_canonical()
    }
}

fn validate_typed_value(value: &TypedValue, depth: usize) -> Result<(), InvalidError> {
    validate_depth(depth)?;
    match value {
        TypedValue::Null
        | TypedValue::Boolean(_)
        | TypedValue::Int(_)
        | TypedValue::Long(_)
        | TypedValue::Date(_)
        | TypedValue::TimestampMicros(_)
        | TypedValue::TimestampTzMicros(_) => Ok(()),
        TypedValue::String(value) => {
            if value.len() > MAX_VALUE_BYTES {
                return Err(InvalidError::new(format!(
                    "string value is {} bytes, exceeds cap {MAX_VALUE_BYTES}",
                    value.len()
                )));
            }
            Ok(())
        }
        TypedValue::Float(value) => validate_float(f64::from(*value)),
        TypedValue::Double(value) => validate_float(*value),
        TypedValue::Decimal(value) => value.validate_canonical(),
        TypedValue::TimeMicros(value) => {
            if !(0..MICROS_PER_DAY).contains(value) {
                return Err(InvalidError::new(format!(
                    "time value {value} is outside one day in microseconds"
                )));
            }
            Ok(())
        }
        TypedValue::Uuid(value) => {
            if value.0.len() != UuidValue::BYTES {
                return Err(InvalidError::new(format!(
                    "UUID must be {} bytes, got {}",
                    UuidValue::BYTES,
                    value.0.len()
                )));
            }
            Ok(())
        }
        TypedValue::Fixed(value) => {
            if value.0.len() > MAX_FIXED_BYTES as usize {
                return Err(InvalidError::new(format!(
                    "binary value is {} bytes, exceeds cap {MAX_FIXED_BYTES}",
                    value.0.len()
                )));
            }
            Ok(())
        }
        TypedValue::Binary(value) => {
            if value.0.len() > MAX_VALUE_BYTES {
                return Err(InvalidError::new(format!(
                    "binary value is {} bytes, exceeds cap {MAX_VALUE_BYTES}",
                    value.0.len()
                )));
            }
            Ok(())
        }
        TypedValue::Struct(values) => {
            if values.len() > MAX_LOGICAL_SCHEMA_FIELDS {
                return Err(InvalidError::new(format!(
                    "struct carries {} fields, exceeds cap {MAX_LOGICAL_SCHEMA_FIELDS}",
                    values.len()
                )));
            }
            let mut previous_id = 0;
            for value in values {
                if value.field_id == 0 || value.field_id <= previous_id {
                    return Err(InvalidError::new(
                        "struct field values must have strictly increasing positive field IDs",
                    ));
                }
                previous_id = value.field_id;
                validate_typed_value(&value.value, depth + 1)?;
            }
            Ok(())
        }
        TypedValue::List(values) => {
            if values.len() > MAX_LOGICAL_SCHEMA_FIELDS {
                return Err(InvalidError::new(format!(
                    "list carries {} values, exceeds cap {MAX_LOGICAL_SCHEMA_FIELDS}",
                    values.len()
                )));
            }
            for value in values {
                validate_typed_value(value, depth + 1)?;
            }
            Ok(())
        }
        TypedValue::Map(entries) => {
            if entries.len() > MAX_LOGICAL_SCHEMA_FIELDS {
                return Err(InvalidError::new(format!(
                    "map carries {} entries, exceeds cap {MAX_LOGICAL_SCHEMA_FIELDS}",
                    entries.len()
                )));
            }
            let mut previous = None;
            for entry in entries {
                validate_typed_value(&entry.key, depth + 1)?;
                validate_typed_value(&entry.value, depth + 1)?;
                let key = canonical_map_key(&entry.key)?;
                if previous.as_ref().is_some_and(|before| before >= &key) {
                    return Err(InvalidError::new(
                        "map entries must be strictly ordered by canonical key",
                    ));
                }
                previous = Some(key);
            }
            Ok(())
        }
    }
}

fn validate_struct_fields(
    fields: &[LogicalField],
    depth: usize,
    ids: &mut BTreeSet<u32>,
    field_count: &mut usize,
    encoded_size: &mut usize,
    allow_provenance: bool,
) -> Result<(), InvalidError> {
    if depth > MAX_LOGICAL_SCHEMA_DEPTH {
        return Err(InvalidError::new(format!(
            "logical schema depth {depth} exceeds cap {MAX_LOGICAL_SCHEMA_DEPTH}"
        )));
    }

    let mut names = BTreeSet::new();
    for field in fields {
        validate_field_identity(field.id, &field.name, ids, &mut names, allow_provenance)?;
        *field_count += 1;
        if *field_count > MAX_LOGICAL_SCHEMA_FIELDS {
            return Err(InvalidError::new(format!(
                "logical schema field count exceeds cap {MAX_LOGICAL_SCHEMA_FIELDS}"
            )));
        }

        let doc_bytes = field.doc.as_ref().map_or(0, String::len);
        if doc_bytes > MAX_FIELD_DOC_BYTES {
            return Err(InvalidError::new(format!(
                "field `{}` documentation is {doc_bytes} bytes, exceeds cap {MAX_FIELD_DOC_BYTES}",
                field.name
            )));
        }
        *encoded_size = encoded_size
            .checked_add(field.name.len() + doc_bytes + 16)
            .ok_or_else(|| InvalidError::new("logical schema size overflow"))?;
        if *encoded_size > MAX_LOGICAL_SCHEMA_BYTES {
            return Err(InvalidError::new(format!(
                "logical schema exceeds cap {MAX_LOGICAL_SCHEMA_BYTES} bytes"
            )));
        }
        validate_logical_type(&field.field_type, depth, ids, field_count, encoded_size)?;
    }
    Ok(())
}

fn validate_field_identity(
    id: u32,
    name: &str,
    ids: &mut BTreeSet<u32>,
    names: &mut BTreeSet<String>,
    allow_provenance: bool,
) -> Result<(), InvalidError> {
    let provenance_pair = PROVENANCE_FIELDS
        .iter()
        .any(|(reserved_id, reserved_name)| *reserved_id == id && *reserved_name == name);
    if id == 0 || (id >= PROVENANCE_FIELD_ID_START && !(allow_provenance && provenance_pair)) {
        return Err(InvalidError::new(format!(
            "field id {id} is not in the user field range"
        )));
    }
    if !ids.insert(id) {
        return Err(InvalidError::new(format!("duplicate field id {id}")));
    }
    validate_field_name(name)?;
    if PROVENANCE_FIELDS
        .iter()
        .any(|(_, reserved)| *reserved == name)
        && !(allow_provenance && provenance_pair)
    {
        return Err(InvalidError::new(format!(
            "field name `{name}` is reserved for provenance"
        )));
    }
    if !names.insert(name.to_owned()) {
        return Err(InvalidError::new(format!(
            "duplicate field name `{name}` in one struct"
        )));
    }
    Ok(())
}

fn validate_field_name(name: &str) -> Result<(), InvalidError> {
    if name.is_empty() {
        return Err(InvalidError::new("field name must not be empty"));
    }
    if name.len() > MAX_FIELD_NAME_BYTES {
        return Err(InvalidError::new(format!(
            "field name is {} bytes, exceeds cap {MAX_FIELD_NAME_BYTES}",
            name.len()
        )));
    }
    if name.trim() != name || name.chars().any(char::is_control) {
        return Err(InvalidError::new(format!(
            "field name `{name}` contains surrounding whitespace or control characters"
        )));
    }
    Ok(())
}

fn validate_logical_type(
    logical_type: &LogicalType,
    depth: usize,
    ids: &mut BTreeSet<u32>,
    field_count: &mut usize,
    encoded_size: &mut usize,
) -> Result<(), InvalidError> {
    match logical_type {
        LogicalType::Decimal { precision, scale } => validate_precision_scale(*precision, *scale),
        LogicalType::Fixed { length } => {
            if *length == 0 || *length > MAX_FIXED_BYTES {
                return Err(InvalidError::new(format!(
                    "fixed length {length} is outside 1..={MAX_FIXED_BYTES}"
                )));
            }
            Ok(())
        }
        LogicalType::Struct { fields } => {
            validate_struct_fields(fields, depth + 1, ids, field_count, encoded_size, false)
        }
        LogicalType::List {
            element_id,
            element,
            ..
        } => {
            validate_nested_id(*element_id, "list element", ids, field_count)?;
            validate_depth(depth + 1)?;
            validate_logical_type(element, depth + 1, ids, field_count, encoded_size)
        }
        LogicalType::Map {
            key_id,
            key,
            value_id,
            value,
            ..
        } => {
            validate_nested_id(*key_id, "map key", ids, field_count)?;
            validate_nested_id(*value_id, "map value", ids, field_count)?;
            if !key.accepts_map_key() {
                return Err(InvalidError::new(
                    "map key type must be a deterministic non-floating primitive",
                ));
            }
            validate_depth(depth + 1)?;
            validate_logical_type(key, depth + 1, ids, field_count, encoded_size)?;
            validate_logical_type(value, depth + 1, ids, field_count, encoded_size)
        }
        _ => Ok(()),
    }
}

fn validate_nested_id(
    id: u32,
    label: &str,
    ids: &mut BTreeSet<u32>,
    field_count: &mut usize,
) -> Result<(), InvalidError> {
    if id == 0 || id >= PROVENANCE_FIELD_ID_START {
        return Err(InvalidError::new(format!(
            "{label} id {id} is not in the user field range"
        )));
    }
    if !ids.insert(id) {
        return Err(InvalidError::new(format!("duplicate {label} id {id}")));
    }
    *field_count += 1;
    if *field_count > MAX_LOGICAL_SCHEMA_FIELDS {
        return Err(InvalidError::new(format!(
            "logical schema field count exceeds cap {MAX_LOGICAL_SCHEMA_FIELDS}"
        )));
    }
    Ok(())
}

fn validate_depth(depth: usize) -> Result<(), InvalidError> {
    if depth > MAX_LOGICAL_SCHEMA_DEPTH {
        return Err(InvalidError::new(format!(
            "logical schema depth {depth} exceeds cap {MAX_LOGICAL_SCHEMA_DEPTH}"
        )));
    }
    Ok(())
}

fn validate_precision_scale(precision: u8, scale: u8) -> Result<(), InvalidError> {
    if precision == 0 || precision > MAX_DECIMAL_PRECISION || scale > precision {
        return Err(InvalidError::new(format!(
            "decimal precision and scale ({precision}, {scale}) are invalid"
        )));
    }
    Ok(())
}

fn validate_decimal(value: &DecimalValue) -> Result<(), InvalidError> {
    validate_precision_scale(value.precision, value.scale)?;
    if value.unscaled.is_empty() || value.unscaled.len() > 16 {
        return Err(InvalidError::new(format!(
            "decimal unscaled value must contain 1..=16 bytes, got {}",
            value.unscaled.len()
        )));
    }
    if value.unscaled.len() > 1 {
        let first = value.unscaled[0];
        let second = value.unscaled[1];
        if (first == 0 && second & 0x80 == 0) || (first == 0xff && second & 0x80 != 0) {
            return Err(InvalidError::new(
                "decimal unscaled bytes are not minimal two's complement",
            ));
        }
    }

    let fill = if value.unscaled[0] & 0x80 == 0 {
        0
    } else {
        0xff
    };
    let mut bytes = [fill; 16];
    let start = bytes.len() - value.unscaled.len();
    bytes[start..].copy_from_slice(&value.unscaled);
    let unscaled = i128::from_be_bytes(bytes);
    let digits = if unscaled == 0 {
        1
    } else {
        unscaled.unsigned_abs().ilog10() + 1
    };
    if digits > u32::from(value.precision) {
        return Err(InvalidError::new(format!(
            "decimal value has {digits} digits, exceeds precision {}",
            value.precision
        )));
    }
    Ok(())
}

fn validate_float(value: f64) -> Result<(), InvalidError> {
    if !value.is_finite() {
        return Err(InvalidError::new("floating-point value must be finite"));
    }
    if value == 0.0 && value.is_sign_negative() {
        return Err(InvalidError::new(
            "negative zero is not a canonical floating-point value",
        ));
    }
    Ok(())
}

fn validate_struct_value(
    values: &[FieldValue],
    fields: &[LogicalField],
) -> Result<(), InvalidError> {
    if values.len() != fields.len() {
        return Err(InvalidError::new(format!(
            "struct carries {} values, schema has {} fields",
            values.len(),
            fields.len()
        )));
    }
    for (value, field) in values.iter().zip(fields) {
        if value.field_id != field.id {
            return Err(InvalidError::new(format!(
                "struct field id {} appears where {} is required",
                value.field_id, field.id
            )));
        }
        value
            .value
            .validate_against(&field.field_type, field.required)?;
    }
    Ok(())
}

fn validate_map(
    entries: &[MapEntry],
    key_type: &LogicalType,
    value_type: &LogicalType,
    value_required: bool,
) -> Result<(), InvalidError> {
    let mut previous = None;
    for entry in entries {
        entry.key.validate_against(key_type, true)?;
        entry.value.validate_against(value_type, value_required)?;
        let key = canonical_map_key(&entry.key)?;
        if previous.as_ref().is_some_and(|before| before >= &key) {
            return Err(InvalidError::new(
                "map entries must be strictly ordered by canonical key",
            ));
        }
        previous = Some(key);
    }
    Ok(())
}

fn canonical_map_key(value: &TypedValue) -> Result<Vec<u8>, InvalidError> {
    let mut key = Vec::new();
    match value {
        TypedValue::Boolean(value) => {
            key.push(0);
            key.push(u8::from(*value));
        }
        TypedValue::Int(value) => {
            key.push(1);
            key.extend_from_slice(&value.to_be_bytes());
        }
        TypedValue::Long(value) => {
            key.push(2);
            key.extend_from_slice(&value.to_be_bytes());
        }
        TypedValue::Decimal(value) => {
            key.push(3);
            key.push(value.precision);
            key.push(value.scale);
            key.extend_from_slice(&value.unscaled);
        }
        TypedValue::Date(value) => {
            key.push(4);
            key.extend_from_slice(&value.to_be_bytes());
        }
        TypedValue::TimeMicros(value) => {
            key.push(5);
            key.extend_from_slice(&value.to_be_bytes());
        }
        TypedValue::TimestampMicros(value) => {
            key.push(6);
            key.extend_from_slice(&value.to_be_bytes());
        }
        TypedValue::TimestampTzMicros(value) => {
            key.push(7);
            key.extend_from_slice(&value.to_be_bytes());
        }
        TypedValue::String(value) => {
            key.push(8);
            key.extend_from_slice(value.as_bytes());
        }
        TypedValue::Uuid(value) => {
            key.push(9);
            key.extend_from_slice(&value.0);
        }
        TypedValue::Fixed(value) => {
            key.push(10);
            key.extend_from_slice(&value.0);
        }
        TypedValue::Binary(value) => {
            key.push(11);
            key.extend_from_slice(&value.0);
        }
        _ => {
            return Err(InvalidError::new(
                "map key must be a deterministic non-floating primitive",
            ));
        }
    }
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields() -> Vec<LogicalField> {
        vec![
            LogicalField {
                id: 1,
                name: "id".to_owned(),
                required: true,
                field_type: LogicalType::Uuid,
                doc: Some("Stable row identity".to_owned()),
            },
            LogicalField {
                id: 2,
                name: "attributes".to_owned(),
                required: false,
                field_type: LogicalType::Map {
                    key_id: 3,
                    key: Box::new(LogicalType::String),
                    value_id: 4,
                    value_required: true,
                    value: Box::new(LogicalType::Long),
                },
                doc: None,
            },
        ]
    }

    #[test]
    fn given_a_logical_schema_when_fingerprinted_then_should_be_deterministic_and_cover_metadata() {
        let schema = LogicalSchema::new(LogicalSchemaId::from_u128(1), 1, fields())
            .expect("schema is valid");
        assert_eq!(
            schema.schema.fingerprint,
            schema.compute_fingerprint().expect("fingerprint")
        );
        schema.validate().expect("fingerprint validates");

        let mut changed_fields = fields();
        changed_fields[0].doc = Some("Different documentation".to_owned());
        let changed = LogicalSchema::new(LogicalSchemaId::from_u128(1), 1, changed_fields)
            .expect("changed schema is valid");
        assert_ne!(schema.schema.fingerprint, changed.schema.fingerprint);
    }

    #[test]
    fn given_reserved_or_duplicate_nested_ids_when_a_schema_is_validated_then_should_reject() {
        let reserved = vec![LogicalField {
            id: PROVENANCE_FIELDS[0].0,
            name: PROVENANCE_FIELDS[0].1.to_owned(),
            required: true,
            field_type: LogicalType::String,
            doc: None,
        }];
        assert!(LogicalSchema::new(LogicalSchemaId::from_u128(1), 1, reserved).is_err());

        let duplicate = vec![LogicalField {
            id: 1,
            name: "items".to_owned(),
            required: true,
            field_type: LogicalType::List {
                element_id: 1,
                element_required: true,
                element: Box::new(LogicalType::String),
            },
            doc: None,
        }];
        assert!(LogicalSchema::new(LogicalSchemaId::from_u128(1), 1, duplicate).is_err());
    }

    #[test]
    fn given_typed_values_when_validated_then_should_enforce_canonical_decimal_float_time_and_map_forms()
     {
        assert!(TypedValue::Double(f64::NAN).validate().is_err());
        assert!(TypedValue::Float(-0.0).validate().is_err());
        assert!(TypedValue::TimeMicros(MICROS_PER_DAY).validate().is_err());
        assert!(
            TypedValue::Decimal(DecimalValue {
                unscaled: vec![0, 1],
                precision: 2,
                scale: 0,
            })
            .validate()
            .is_err()
        );
        assert!(
            TypedValue::Map(vec![
                MapEntry {
                    key: TypedValue::String("b".to_owned()),
                    value: TypedValue::Long(1),
                },
                MapEntry {
                    key: TypedValue::String("a".to_owned()),
                    value: TypedValue::Long(2),
                },
            ])
            .validate()
            .is_err()
        );
    }

    #[test]
    fn given_ids_and_binary_values_when_round_tripped_through_json_then_should_be_lossless() {
        let id = LogicalSchemaId::from_u128(u128::MAX - 7);
        let json = serde_json::to_string(&id).expect("id serializes");
        let decoded: LogicalSchemaId = serde_json::from_str(&json).expect("id deserializes");
        assert_eq!(decoded, id);

        let value = TypedValue::Binary(BinaryValue(vec![0, 127, 255]));
        let json = serde_json::to_string(&value).expect("value serializes");
        let decoded: TypedValue = serde_json::from_str(&json).expect("value deserializes");
        assert_eq!(decoded, value);
    }
}
