use super::eval::{DecodeLimits, FilterRecord, HeaderValueRef, PathTrie};
use super::{ConsumerFilter, FaultReason, FilterCodec};
use crate::control::{SchemaDef, SchemaSource};
use crate::error::InvalidError;
use crate::limits::{MAX_FILTER_SCHEMA_BYTES, MAX_FILTER_SCHEMA_DEPTH};
use apache_avro::Schema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::schema::{InnerDecimalSchema, ResolvedSchema, UuidSchema};
use apache_avro::types::Value as Avro;
use prost_reflect::{
    DescriptorPool, DynamicMessage, Kind, MapKey, MessageDescriptor, ReflectMessage, Value as Proto,
};
use serde_json::{Map, Number, Value};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

/// Schema decoders prepared before records enter the evaluation loop.
#[derive(Clone, Debug, Default)]
pub struct PayloadDecoders {
    schemas: BTreeMap<u32, WriterSchema>,
}

#[derive(Clone, Debug)]
enum WriterSchema {
    Avro(Arc<AvroDecoder>),
    Protobuf(MessageDescriptor),
}

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

impl fmt::Debug for AvroDecoder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AvroDecoder")
            .field("schema", self.borrow_schema())
            .finish_non_exhaustive()
    }
}

impl AvroDecoder {
    fn compile(source: &str) -> Result<Self, String> {
        // Both validation and decoding borrow one immutable schema tree. Named
        // references are resolved once without cloning nested schemas.
        AvroDecoderTryBuilder {
            schema: physical_avro_schema(source)?,
            names_builder: |schema| ResolvedSchema::new(schema),
            reader_builder: |schema| GenericDatumReader::builder(schema).build(),
        }
        .try_build()
        .map_err(|error| error.to_string())
    }
}

impl PayloadDecoders {
    pub fn compile(filter: &ConsumerFilter, schemas: &[SchemaDef]) -> Result<Self, InvalidError> {
        let mut prepared = BTreeMap::new();
        for id in &filter.schema_refs {
            let schema = schemas
                .iter()
                .find(|schema| schema.id == *id)
                .ok_or_else(|| InvalidError::new(format!("writer schema {id} was not supplied")))?;
            let decoder = match (&schema.source, filter.codec) {
                (SchemaSource::Avro { schema }, FilterCodec::Avro) => {
                    WriterSchema::Avro(Arc::new(AvroDecoder::compile(schema).map_err(|error| {
                        InvalidError::new(format!("Avro schema {id}: {error}"))
                    })?))
                }
                (
                    SchemaSource::Protobuf {
                        descriptor_set,
                        message_type,
                    },
                    FilterCodec::Protobuf,
                ) => {
                    if descriptor_set.len() > MAX_FILTER_SCHEMA_BYTES {
                        return Err(InvalidError::new(format!(
                            "Protobuf descriptor {id} exceeds {MAX_FILTER_SCHEMA_BYTES} bytes"
                        )));
                    }
                    let pool =
                        DescriptorPool::decode(descriptor_set.as_slice()).map_err(|error| {
                            InvalidError::new(format!("Protobuf schema {id}: {error}"))
                        })?;
                    WriterSchema::Protobuf(
                        pool.get_message_by_name(message_type.trim_start_matches('.'))
                            .ok_or_else(|| {
                                InvalidError::new(format!(
                                    "schema {id} has no message {message_type}"
                                ))
                            })?,
                    )
                }
                _ => {
                    return Err(InvalidError::new(format!(
                        "schema {id} does not match codec {}",
                        filter.codec
                    )));
                }
            };
            prepared.insert(*id, decoder);
        }
        Ok(Self { schemas: prepared })
    }

    pub fn decode(
        &self,
        codec: FilterCodec,
        record: &FilterRecord<'_>,
        limits: &DecodeLimits,
    ) -> Result<Value, FaultReason> {
        self.decode_paths(codec, record, limits, &PathTrie::whole())
    }

    pub(super) fn decode_paths(
        &self,
        codec: FilterCodec,
        record: &FilterRecord<'_>,
        limits: &DecodeLimits,
        paths: &PathTrie,
    ) -> Result<Value, FaultReason> {
        if record.payload.len() > limits.max_payload_bytes {
            return Err(FaultReason::TooLarge);
        }
        if codec == FilterCodec::Cbor {
            return decode_cbor(record.payload, limits);
        }
        let id = record
            .headers
            .iter()
            .rev()
            .find(|header| header.key == crate::headers::SCHEMA_ID)
            .and_then(|header| match header.value {
                HeaderValueRef::Uint(id) => u32::try_from(id).ok(),
                _ => None,
            })
            .ok_or(FaultReason::MissingSchema)?;
        match self.schemas.get(&id).ok_or(FaultReason::SchemaNotAllowed)? {
            WriterSchema::Avro(decoder) if codec == FilterCodec::Avro => {
                let mut input = Input::new(record.payload, limits);
                input.avro(
                    decoder.borrow_schema(),
                    decoder.borrow_names().get_names(),
                    0,
                )?;
                input.finish()?;
                let mut bytes = record.payload;
                let decoded = decoder
                    .borrow_reader()
                    .read_value(&mut bytes)
                    .map_err(|_| FaultReason::Malformed)?;
                avro_value(decoded, paths)
            }
            WriterSchema::Protobuf(descriptor) if codec == FilterCodec::Protobuf => {
                let mut input = Input::new(record.payload, limits);
                input.protobuf(descriptor, 0)?;
                let message = DynamicMessage::decode(descriptor.clone(), record.payload)
                    .map_err(|_| FaultReason::Malformed)?;
                protobuf_message(&message)
            }
            _ => Err(FaultReason::SchemaMismatch),
        }
    }
}

/// Decode exactly one self-describing CBOR item with bounded recursion.
pub fn decode_cbor(payload: &[u8], limits: &DecodeLimits) -> Result<Value, FaultReason> {
    if payload.len() > limits.max_payload_bytes {
        return Err(FaultReason::TooLarge);
    }
    let mut checked = Input::new(payload, limits);
    checked.cbor(0)?;
    checked.finish()?;
    let mut bytes = payload;
    let value = ciborium::de::from_reader_with_recursion_limit::<super::cbor::CborValue, _>(
        &mut bytes,
        limits
            .max_depth
            .min(crate::limits::MAX_FILTER_PARSE_DEPTH)
            .saturating_add(2),
    )
    .map_err(|error| match error {
        ciborium::de::Error::RecursionLimitExceeded => FaultReason::TooDeep,
        _ => FaultReason::Malformed,
    })?;
    if !bytes.is_empty() {
        return Err(FaultReason::Malformed);
    }
    Ok(value.0)
}

#[cfg(test)]
fn cbor_value(value: ciborium::Value) -> Result<Value, FaultReason> {
    Ok(match value {
        ciborium::Value::Null => Value::Null,
        ciborium::Value::Bool(value) => Value::Bool(value),
        ciborium::Value::Integer(value) => {
            let integer = i128::from(value);
            if let Ok(value) = i64::try_from(integer) {
                Value::from(value)
            } else if let Ok(value) = u64::try_from(integer) {
                Value::from(value)
            } else {
                return Err(FaultReason::Malformed);
            }
        }
        ciborium::Value::Float(value) => number(value)?,
        ciborium::Value::Text(value) => Value::String(value),
        ciborium::Value::Bytes(value) => bytes_value(&value),
        ciborium::Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(cbor_value)
                .collect::<Result<_, _>>()?,
        ),
        ciborium::Value::Map(items) => {
            let mut object = Map::new();
            for (key, value) in items {
                let ciborium::Value::Text(key) = key else {
                    return Err(FaultReason::Malformed);
                };
                if object.insert(key, cbor_value(value)?).is_some() {
                    return Err(FaultReason::Malformed);
                }
            }
            Value::Object(object)
        }
        // Tags need an explicit semantic profile, never silently unwrap them.
        _ => return Err(FaultReason::Malformed),
    })
}

fn physical_avro_schema(schema: &str) -> Result<Schema, String> {
    // Logical annotations describe producer interpretation. Comparisons use
    // physical values and explicit coercions, identically in every SDK.
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
    let mut value = super::eval::decode_json(
        schema.as_bytes(),
        &DecodeLimits {
            max_payload_bytes: MAX_FILTER_SCHEMA_BYTES,
            max_depth: MAX_FILTER_SCHEMA_DEPTH,
        },
    )
    .map_err(|reason| format!("schema JSON is {reason}"))?;
    strip(&mut value);
    Schema::parse_str(&value.to_string()).map_err(|error| error.to_string())
}

fn number(value: f64) -> Result<Value, FaultReason> {
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or(FaultReason::Malformed)
}

fn bytes_value(bytes: &[u8]) -> Value {
    Value::Array(bytes.iter().copied().map(Value::from).collect())
}

fn avro_value(value: apache_avro::types::Value, paths: &PathTrie) -> Result<Value, FaultReason> {
    let whole = PathTrie::whole();
    Ok(match value {
        Avro::Null => Value::Null,
        Avro::Boolean(value) => Value::Bool(value),
        Avro::Int(value) => Value::from(value),
        Avro::Long(value) => Value::from(value),
        Avro::Float(value) => number(f64::from(value))?,
        Avro::Double(value) => number(value)?,
        Avro::String(value) | Avro::Enum(_, value) => Value::String(value),
        Avro::Bytes(value) | Avro::Fixed(_, value) => bytes_value(&value),
        Avro::Union(_, value) => avro_value(*value, paths)?,
        Avro::Record(fields) => Value::Object(
            fields
                .into_iter()
                .filter_map(|(key, value)| {
                    let selected = if paths.whole {
                        Some(&whole)
                    } else {
                        paths.child(&key)
                    };
                    selected.map(|child| avro_value(value, child).map(|value| (key, value)))
                })
                .collect::<Result<_, _>>()?,
        ),
        Avro::Map(fields) => Value::Object(
            fields
                .into_iter()
                .filter_map(|(key, value)| {
                    let selected = if paths.whole {
                        Some(&whole)
                    } else {
                        paths.child(&key)
                    };
                    selected.map(|child| avro_value(value, child).map(|value| (key, value)))
                })
                .collect::<Result<_, _>>()?,
        ),
        Avro::Array(values) => {
            let mut selected = Vec::new();
            for (index, value) in values.into_iter().enumerate() {
                let child = if paths.whole {
                    Some(&whole)
                } else {
                    u32::try_from(index)
                        .ok()
                        .and_then(|index| paths.index(index))
                };
                if let Some(child) = child {
                    selected.resize(index, Value::Null);
                    selected.push(avro_value(value, child)?);
                }
            }
            Value::Array(selected)
        }
        _ => return Err(FaultReason::Malformed),
    })
}

// A field without presence tracking (a proto3 scalar or any repeated field)
// is always part of the message, at its default when the bytes omit it, so
// `counter == 0` and `present("values")` hold as they do for JSON and Avro.
fn protobuf_message(message: &DynamicMessage) -> Result<Value, FaultReason> {
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
        Proto::Message(value) => protobuf_message(value)?,
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

// Validate nesting and collection sizes before allocating decoder trees.
// Avro can represent huge arrays of null with only a few bytes.
struct Input<'a> {
    bytes: &'a [u8],
    limits: &'a DecodeLimits,
    remaining_values: usize,
}

impl<'a> Input<'a> {
    fn new(bytes: &'a [u8], limits: &'a DecodeLimits) -> Self {
        Self {
            bytes,
            limits,
            remaining_values: limits.max_payload_bytes.max(1),
        }
    }

    fn finish(&self) -> Result<(), FaultReason> {
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
        if depth
            > self
                .limits
                .max_depth
                .min(crate::limits::MAX_FILTER_PARSE_DEPTH)
        {
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
        names: &apache_avro::schema::NamesRef<'_>,
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
        names: &apache_avro::schema::NamesRef<'_>,
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

    fn cbor(&mut self, depth: usize) -> Result<(), FaultReason> {
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

#[cfg(test)]
mod cbor_tests {
    use super::*;
    use ciborium::Value as Cbor;

    #[test]
    fn given_cbor_scalars_and_collections_when_decoded_directly_then_should_match_the_previous_value_rules()
     {
        let values = [
            Cbor::Integer(i64::MIN.into()),
            Cbor::Integer(u64::MAX.into()),
            Cbor::Float(-0.0),
            Cbor::Float(0.25),
            Cbor::Float(f64::NAN),
            Cbor::Float(f64::INFINITY),
            Cbor::Bytes(vec![0, 128, 255]),
            Cbor::Text("λ".to_owned()),
            Cbor::Array(vec![
                Cbor::Null,
                Cbor::Bool(true),
                Cbor::Array(vec![Cbor::Integer(7.into())]),
            ]),
            Cbor::Map(vec![(
                Cbor::Text("key".to_owned()),
                Cbor::Integer(42.into()),
            )]),
            Cbor::Map(vec![(Cbor::Integer(1.into()), Cbor::Null)]),
            Cbor::Map(vec![
                (Cbor::Text("key".to_owned()), Cbor::Null),
                (Cbor::Text("key".to_owned()), Cbor::Bool(true)),
            ]),
            Cbor::Tag(1, Box::new(Cbor::Integer(0.into()))),
        ];
        for value in values {
            let expected = cbor_value(value.clone());
            let mut bytes = Vec::new();
            ciborium::ser::into_writer(&value, &mut bytes).expect("CBOR fixture");
            assert_eq!(
                decode_cbor(&bytes, &DecodeLimits::default()),
                expected,
                "{value:?}"
            );
        }
    }

    #[test]
    fn given_cbor_bytes_when_decoded_then_should_preserve_exact_unsigned_values_and_byte_arrays() {
        let bytes = [
            0x82, 0x1b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x42, 0x00, 0xff,
        ];
        assert_eq!(
            decode_cbor(&bytes, &DecodeLimits::default()).expect("bounded CBOR"),
            serde_json::json!([u64::MAX, [0, 255]])
        );
        for malformed in [
            &[0x61, 0xff][..],
            &[0x9a, 1, 0, 0, 0][..],
            &[0x3b, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff][..],
        ] {
            assert_eq!(
                decode_cbor(malformed, &DecodeLimits::default()),
                Err(FaultReason::Malformed)
            );
        }
    }
}
