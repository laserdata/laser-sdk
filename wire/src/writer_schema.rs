use crate::content::ContentType;
use crate::control::SchemaSource;
use crate::error::InvalidError;
use crate::filter::FaultReason;
use crate::limits::{
    MAX_FILTER_PARSE_DEPTH, MAX_FILTER_SCHEMA_BYTES, MAX_FILTER_SCHEMA_DEPTH,
    MAX_PROJECTOR_PAYLOAD_BYTES,
};
use crate::validate::json_depth_within;
use apache_avro::Schema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::schema::{InnerDecimalSchema, NamesRef, ResolvedSchema, UuidSchema};
use apache_avro::types::Value as Avro;
use prost_reflect::{
    DescriptorPool, DynamicMessage, Kind, MapKey, MessageDescriptor, ReflectMessage, Value as Proto,
};
use serde_json::{Map, Number, Value};
use std::fmt;
use std::sync::Arc;

/// The bounds a stored record decodes under, the projector's payload cap.
pub const RECORD_DECODE_LIMITS: DecodeLimits = DecodeLimits {
    max_payload_bytes: MAX_PROJECTOR_PAYLOAD_BYTES,
    max_depth: MAX_FILTER_PARSE_DEPTH,
};

/// Bounds on decoding one payload. Checked before the payload is parsed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeLimits {
    pub max_payload_bytes: usize,
    pub max_depth: usize,
}

/// A compiled Avro or Protobuf writer schema, decoding into the one shared model.
#[derive(Clone, Debug)]
pub enum WriterSchema {
    Avro(AvroWriterSchema),
    Protobuf(MessageDescriptor),
}

/// An Avro writer schema without logical annotations, so values stay physical.
#[derive(Clone)]
pub struct AvroWriterSchema(Arc<AvroDecoder>);

#[ouroboros::self_referencing]
struct AvroDecoder {
    schema: Schema,
    #[borrows(schema)]
    #[covariant]
    names: ResolvedSchema<'this>,
    #[borrows(schema)]
    #[covariant]
    reader: GenericDatumReader<'this>,
}

// Validates nesting and collection sizes before a decoder allocates its tree.
// Avro can represent huge arrays of null with only a few bytes.
pub(crate) struct Input<'a> {
    bytes: &'a [u8],
    limits: &'a DecodeLimits,
    remaining_values: usize,
}

/// The whole decoded value, every field kept.
struct Whole;

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: 1024 * 1024,
            max_depth: 64,
        }
    }
}

impl WriterSchema {
    /// Compile an Avro or Protobuf source within the shared schema bounds.
    pub fn compile(source: &SchemaSource) -> Result<Self, InvalidError> {
        match source {
            SchemaSource::Avro { schema } => AvroWriterSchema::compile(schema).map(Self::Avro),
            SchemaSource::Protobuf {
                descriptor_set,
                message_type,
            } => compile_protobuf(descriptor_set, message_type).map(Self::Protobuf),
            SchemaSource::JsonSchema { .. } => Err(InvalidError::new(
                "a JSON Schema validates payloads and has no writer decoder",
            )),
            SchemaSource::Unknown => Err(InvalidError::new("unknown schema source kind")),
        }
    }

    /// The `agdx.ct` a record written under this schema carries.
    pub fn content_type(&self) -> ContentType {
        match self {
            Self::Avro(_) => ContentType::Avro,
            Self::Protobuf(_) => ContentType::Protobuf,
        }
    }

    /// Decode exactly one body within `limits` into the shared model.
    pub fn decode(&self, payload: &[u8], limits: &DecodeLimits) -> Result<Value, FaultReason> {
        match self {
            Self::Avro(schema) => schema.decode(payload, limits),
            Self::Protobuf(descriptor) => decode_protobuf(descriptor, payload, limits),
        }
    }
}

impl AvroWriterSchema {
    /// Compile Avro JSON text of at most 1 MiB and nesting 64.
    pub fn compile(source: &str) -> Result<Self, InvalidError> {
        AvroDecoderTryBuilder {
            schema: physical_avro_schema(source)?,
            names_builder: |schema| ResolvedSchema::new(schema),
            reader_builder: |schema| GenericDatumReader::builder(schema).build(),
        }
        .try_build()
        .map(|decoder| Self(Arc::new(decoder)))
        .map_err(|error| InvalidError::new(format!("Avro schema does not compile: {error}")))
    }

    /// The physical schema bodies are read and written with.
    pub fn schema(&self) -> &Schema {
        self.0.borrow_schema()
    }

    /// Decode exactly one datum within `limits` into the shared model.
    pub fn decode(&self, payload: &[u8], limits: &DecodeLimits) -> Result<Value, FaultReason> {
        self.decode_selected(payload, limits, &Whole)
    }

    pub(crate) fn decode_selected<S: Selection>(
        &self,
        payload: &[u8],
        limits: &DecodeLimits,
        selection: &S,
    ) -> Result<Value, FaultReason> {
        if payload.len() > limits.max_payload_bytes {
            return Err(FaultReason::TooLarge);
        }
        let mut input = Input::new(payload, limits);
        input.avro(self.schema(), self.0.borrow_names().get_names(), 0)?;
        input.finish()?;
        let mut bytes = payload;
        let decoded = self
            .0
            .borrow_reader()
            .read_value(&mut bytes)
            .map_err(|_| FaultReason::Malformed)?;
        lower_avro_selected(decoded, selection)
    }
}

impl fmt::Debug for AvroWriterSchema {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("AvroWriterSchema")
            .field(self.schema())
            .finish()
    }
}

impl<'a> Input<'a> {
    pub(crate) fn new(bytes: &'a [u8], limits: &'a DecodeLimits) -> Self {
        Self {
            bytes,
            limits,
            remaining_values: limits.max_payload_bytes.max(1),
        }
    }

    pub(crate) fn finish(&self) -> Result<(), FaultReason> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(FaultReason::Malformed)
        }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], FaultReason> {
        let result = self.bytes.get(..count).ok_or(FaultReason::Malformed)?;
        self.bytes = &self.bytes[count..];
        Ok(result)
    }

    fn varint(&mut self) -> Result<u64, FaultReason> {
        let mut result = 0u64;
        for shift in (0..70).step_by(7) {
            let byte = self.take(1)?[0];
            if shift == 63 && byte > 1 {
                return Err(FaultReason::Malformed);
            }
            result |= u64::from(byte & 127) << shift;
            if byte < 128 {
                return Ok(result);
            }
        }
        Err(FaultReason::Malformed)
    }

    fn long(&mut self) -> Result<i64, FaultReason> {
        let raw = self.varint()?;
        Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64))
    }

    fn sized(&mut self) -> Result<(), FaultReason> {
        let count = usize::try_from(self.long()?).map_err(|_| FaultReason::Malformed)?;
        self.take(count)?;
        Ok(())
    }

    fn visit(&mut self, depth: usize) -> Result<(), FaultReason> {
        if depth > self.limits.max_depth.min(MAX_FILTER_PARSE_DEPTH) {
            return Err(FaultReason::TooDeep);
        }
        self.remaining_values = self
            .remaining_values
            .checked_sub(1)
            .ok_or(FaultReason::TooLarge)?;
        Ok(())
    }

    fn avro(
        &mut self,
        schema: &Schema,
        names: &NamesRef<'_>,
        depth: usize,
    ) -> Result<(), FaultReason> {
        self.visit(depth)?;
        match schema {
            Schema::Null => {}
            Schema::Boolean => {
                if self.take(1)?[0] > 1 {
                    return Err(FaultReason::Malformed);
                }
            }
            Schema::Int
            | Schema::Long
            | Schema::Date
            | Schema::TimeMillis
            | Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos
            | Schema::Enum(_) => {
                self.long()?;
            }
            Schema::Float => {
                let bytes: [u8; 4] = self
                    .take(4)?
                    .try_into()
                    .map_err(|_| FaultReason::Malformed)?;
                if !f32::from_le_bytes(bytes).is_finite() {
                    return Err(FaultReason::Malformed);
                }
            }
            Schema::Double => {
                let bytes: [u8; 8] = self
                    .take(8)?
                    .try_into()
                    .map_err(|_| FaultReason::Malformed)?;
                if !f64::from_le_bytes(bytes).is_finite() {
                    return Err(FaultReason::Malformed);
                }
            }
            Schema::Bytes | Schema::String | Schema::BigDecimal => self.sized()?,
            Schema::Fixed(fixed) | Schema::Duration(fixed) => {
                self.take(fixed.size)?;
            }
            Schema::Decimal(decimal) => match &decimal.inner {
                InnerDecimalSchema::Bytes => self.sized()?,
                InnerDecimalSchema::Fixed(fixed) => {
                    self.take(fixed.size)?;
                }
            },
            Schema::Uuid(uuid) => match uuid {
                UuidSchema::Fixed(fixed) => {
                    self.take(fixed.size)?;
                }
                _ => self.sized()?,
            },
            Schema::Union(union) => {
                let index = usize::try_from(self.long()?).map_err(|_| FaultReason::Malformed)?;
                self.avro(
                    union.variants().get(index).ok_or(FaultReason::Malformed)?,
                    names,
                    depth + 1,
                )?;
            }
            Schema::Record(record) => {
                for field in &record.fields {
                    self.avro(&field.schema, names, depth + 1)?;
                }
            }
            Schema::Ref { name } => {
                self.avro(names.get(name).ok_or(FaultReason::Malformed)?, names, depth)?
            }
            Schema::Array(array) => self.blocks(&array.items, names, depth, false)?,
            Schema::Map(map) => self.blocks(&map.types, names, depth, true)?,
        }
        Ok(())
    }

    fn blocks(
        &mut self,
        item: &Schema,
        names: &NamesRef<'_>,
        depth: usize,
        map: bool,
    ) -> Result<(), FaultReason> {
        loop {
            let count = self.long()?;
            if count == 0 {
                return Ok(());
            }
            let block_end = if count < 0 {
                let size = usize::try_from(self.long()?).map_err(|_| FaultReason::Malformed)?;
                Some(
                    self.bytes
                        .len()
                        .checked_sub(size)
                        .ok_or(FaultReason::Malformed)?,
                )
            } else {
                None
            };
            let count = usize::try_from(count.checked_abs().ok_or(FaultReason::TooLarge)?)
                .map_err(|_| FaultReason::TooLarge)?;
            if count > self.remaining_values {
                return Err(FaultReason::TooLarge);
            }
            for _ in 0..count {
                if map {
                    self.sized()?;
                }
                self.avro(item, names, depth + 1)?;
            }
            if block_end.is_some_and(|end| self.bytes.len() != end) {
                return Err(FaultReason::Malformed);
            }
        }
    }

    #[cfg(feature = "filter-eval")]
    pub(crate) fn cbor(&mut self, depth: usize) -> Result<(), FaultReason> {
        self.visit(depth)?;
        let head = self.take(1)?[0];
        let major = head >> 5;
        let info = head & 31;
        if major == 6 || info == 31 {
            return Err(FaultReason::Malformed);
        }
        let mut value = u64::from(info);
        if info >= 24 {
            if info > 27 {
                return Err(FaultReason::Malformed);
            }
            value = 0;
            for byte in self.take(1 << (info - 24))? {
                value = (value << 8) | u64::from(*byte);
            }
        }
        if major == 2 || major == 3 {
            self.take(usize::try_from(value).map_err(|_| FaultReason::Malformed)?)?;
        }
        if major == 4 || major == 5 {
            let count = value
                .checked_mul(if major == 5 { 2 } else { 1 })
                .ok_or(FaultReason::Malformed)?;
            if count > self.bytes.len() as u64 {
                return Err(FaultReason::Malformed);
            }
            for _ in 0..count {
                self.cbor(depth + 1)?;
            }
        }
        Ok(())
    }

    fn protobuf(
        &mut self,
        descriptor: &MessageDescriptor,
        depth: usize,
    ) -> Result<(), FaultReason> {
        self.visit(depth)?;
        while !self.bytes.is_empty() {
            let key = self.varint()?;
            let field = u32::try_from(key >> 3).map_err(|_| FaultReason::Malformed)?;
            if field == 0 || field > 0x1fff_ffff {
                return Err(FaultReason::Malformed);
            }
            match key & 7 {
                0 => {
                    self.varint()?;
                }
                1 => {
                    self.take(8)?;
                }
                2 => {
                    let count =
                        usize::try_from(self.varint()?).map_err(|_| FaultReason::TooLarge)?;
                    let data = self.take(count)?;
                    if let Some(field) = descriptor.get_field(field)
                        && let Kind::Message(nested) = field.kind()
                    {
                        let mut child = Self {
                            bytes: data,
                            limits: self.limits,
                            remaining_values: self.remaining_values,
                        };
                        child.protobuf(&nested, depth + 1)?;
                        self.remaining_values = child.remaining_values;
                    }
                }
                5 => {
                    self.take(4)?;
                }
                _ => return Err(FaultReason::Malformed),
            }
        }
        Ok(())
    }
}

/// The part of a decoded value a reader keeps.
pub(crate) trait Selection {
    fn field(&self, key: &str) -> Option<&Self>;
    fn item(&self, index: u32) -> Option<&Self>;
}

impl Selection for Whole {
    fn field(&self, _key: &str) -> Option<&Self> {
        Some(self)
    }

    fn item(&self, _index: u32) -> Option<&Self> {
        Some(self)
    }
}

/// Resolve a Protobuf message type in a descriptor set of at most 1 MiB.
pub fn compile_protobuf(
    descriptor_set: &[u8],
    message_type: &str,
) -> Result<MessageDescriptor, InvalidError> {
    if descriptor_set.len() > MAX_FILTER_SCHEMA_BYTES {
        return Err(InvalidError::new(format!(
            "Protobuf descriptor set exceeds {MAX_FILTER_SCHEMA_BYTES} bytes"
        )));
    }
    let pool = DescriptorPool::decode(descriptor_set).map_err(|error| {
        InvalidError::new(format!("Protobuf descriptor set does not decode: {error}"))
    })?;
    pool.get_message_by_name(message_type.trim_start_matches('.'))
        .ok_or_else(|| InvalidError::new(format!("descriptor set has no message `{message_type}`")))
}

/// Decode exactly one Protobuf message within `limits` into the shared model.
pub fn decode_protobuf(
    descriptor: &MessageDescriptor,
    payload: &[u8],
    limits: &DecodeLimits,
) -> Result<Value, FaultReason> {
    if payload.len() > limits.max_payload_bytes {
        return Err(FaultReason::TooLarge);
    }
    Input::new(payload, limits).protobuf(descriptor, 0)?;
    let message =
        DynamicMessage::decode(descriptor.clone(), payload).map_err(|_| FaultReason::Malformed)?;
    lower_protobuf(&message)
}

/// Lower an Avro value read under its physical schema into the shared model.
pub fn lower_avro(value: Avro) -> Result<Value, FaultReason> {
    lower_avro_selected(value, &Whole)
}

/// Lower a Protobuf message with schema field names, numeric enums and exact integers.
pub fn lower_protobuf(message: &DynamicMessage) -> Result<Value, FaultReason> {
    // A field without presence tracking (a proto3 scalar or any repeated
    // field) is always part of the message, at its default when the bytes
    // omit it, so `counter == 0` and `present("values")` hold as for JSON.
    let mut object = Map::new();
    for field in message.descriptor().fields() {
        let set = message.has_field(&field);
        if field.is_required() && !set {
            return Err(FaultReason::Malformed);
        }
        if set || !field.supports_presence() {
            object.insert(
                field.name().to_owned(),
                protobuf_value(&message.get_field(&field))?,
            );
        }
    }
    Ok(Value::Object(object))
}

pub(crate) fn lower_avro_selected<S: Selection>(
    value: Avro,
    selection: &S,
) -> Result<Value, FaultReason> {
    Ok(match value {
        Avro::Null => Value::Null,
        Avro::Boolean(value) => Value::Bool(value),
        Avro::Int(value) => Value::from(value),
        Avro::Long(value) => Value::from(value),
        Avro::Float(value) => number(f64::from(value))?,
        Avro::Double(value) => number(value)?,
        Avro::String(value) | Avro::Enum(_, value) => Value::String(value),
        Avro::Bytes(value) | Avro::Fixed(_, value) => bytes_value(&value),
        Avro::Union(_, value) => lower_avro_selected(*value, selection)?,
        Avro::Record(fields) => Value::Object(
            fields
                .into_iter()
                .filter_map(|(key, value)| {
                    selection
                        .field(&key)
                        .map(|child| lower_avro_selected(value, child).map(|value| (key, value)))
                })
                .collect::<Result<_, _>>()?,
        ),
        Avro::Map(fields) => Value::Object(
            fields
                .into_iter()
                .filter_map(|(key, value)| {
                    selection
                        .field(&key)
                        .map(|child| lower_avro_selected(value, child).map(|value| (key, value)))
                })
                .collect::<Result<_, _>>()?,
        ),
        Avro::Array(values) => {
            let mut selected = Vec::new();
            for (index, value) in values.into_iter().enumerate() {
                let child = u32::try_from(index)
                    .ok()
                    .and_then(|index| selection.item(index));
                if let Some(child) = child {
                    selected.resize(index, Value::Null);
                    selected.push(lower_avro_selected(value, child)?);
                }
            }
            Value::Array(selected)
        }
        _ => return Err(FaultReason::Malformed),
    })
}

pub(crate) fn number(value: f64) -> Result<Value, FaultReason> {
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or(FaultReason::Malformed)
}

pub(crate) fn bytes_value(bytes: &[u8]) -> Value {
    Value::Array(bytes.iter().copied().map(Value::from).collect())
}

fn physical_avro_schema(source: &str) -> Result<Schema, InvalidError> {
    // Logical annotations describe producer interpretation. Values stay
    // physical, identically in every SDK and on the server.
    fn strip(value: &mut Value) {
        match value {
            Value::Object(map) => {
                map.remove("logicalType");
                for value in map.values_mut() {
                    strip(value);
                }
            }
            Value::Array(items) => {
                for value in items {
                    strip(value);
                }
            }
            _ => {}
        }
    }
    if source.len() > MAX_FILTER_SCHEMA_BYTES {
        return Err(InvalidError::new(format!(
            "Avro schema exceeds {MAX_FILTER_SCHEMA_BYTES} bytes"
        )));
    }
    if !json_depth_within(source.as_bytes(), MAX_FILTER_SCHEMA_DEPTH) {
        return Err(InvalidError::new(format!(
            "Avro schema nests deeper than {MAX_FILTER_SCHEMA_DEPTH}"
        )));
    }
    let mut value: Value = serde_json::from_str(source)
        .map_err(|error| InvalidError::new(format!("Avro schema is not valid JSON: {error}")))?;
    strip(&mut value);
    Schema::parse_str(&value.to_string())
        .map_err(|error| InvalidError::new(format!("Avro schema does not compile: {error}")))
}

fn protobuf_value(value: &Proto) -> Result<Value, FaultReason> {
    Ok(match value {
        Proto::Bool(value) => Value::Bool(*value),
        Proto::I32(value) | Proto::EnumNumber(value) => Value::from(*value),
        Proto::I64(value) => Value::from(*value),
        Proto::U32(value) => Value::from(*value),
        Proto::U64(value) => Value::from(*value),
        Proto::F32(value) => number(f64::from(*value))?,
        Proto::F64(value) => number(*value)?,
        Proto::String(value) => Value::String(value.clone()),
        Proto::Bytes(value) => bytes_value(value),
        Proto::Message(value) => lower_protobuf(value)?,
        Proto::List(values) => Value::Array(
            values
                .iter()
                .map(protobuf_value)
                .collect::<Result<_, _>>()?,
        ),
        Proto::Map(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| {
                    let key = match key {
                        MapKey::Bool(value) => value.to_string(),
                        MapKey::I32(value) => value.to_string(),
                        MapKey::I64(value) => value.to_string(),
                        MapKey::U32(value) => value.to_string(),
                        MapKey::U64(value) => value.to_string(),
                        MapKey::String(value) => value.clone(),
                    };
                    Ok((key, protobuf_value(value)?))
                })
                .collect::<Result<_, FaultReason>>()?,
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_an_avro_schema_past_the_nesting_bound_when_compiled_then_should_refuse_it() {
        let deep = format!(
            "{}\"long\"{}",
            "{\"type\":\"array\",\"items\":".repeat(MAX_FILTER_SCHEMA_DEPTH + 1),
            "}".repeat(MAX_FILTER_SCHEMA_DEPTH + 1)
        );
        let error = AvroWriterSchema::compile(&deep).expect_err("the schema nests too deep");
        assert!(error.to_string().contains("nests deeper"), "{error}");
    }

    #[test]
    fn given_a_logical_decimal_when_decoded_then_should_lower_its_unscaled_bytes() {
        let schema = AvroWriterSchema::compile(
            r#"{"type":"bytes","logicalType":"decimal","precision":8,"scale":2}"#,
        )
        .expect("the schema compiles");
        assert_eq!(
            schema
                .decode(&[4, 0x30, 0x39], &DecodeLimits::default())
                .expect("the datum decodes"),
            serde_json::json!([48, 57])
        );
    }

    #[test]
    fn given_a_truncated_avro_datum_when_decoded_then_should_refuse_it_as_malformed() {
        let schema = AvroWriterSchema::compile(
            r#"{"type":"record","name":"Order","fields":[{"name":"customer","type":"string"}]}"#,
        )
        .expect("the schema compiles");
        assert_eq!(
            schema.decode(&[10, b'a', b'l'], &DecodeLimits::default()),
            Err(FaultReason::Malformed)
        );
    }
}
