use super::eval::{DecodeLimits, FilterRecord, HeaderValueRef, PathTrie};
use super::{ConsumerFilter, FaultReason, FilterCodec};
use crate::control::{SchemaDef, SchemaSource};
use crate::error::InvalidError;
use crate::writer_schema::{Input, WriterSchema, decode_protobuf};
#[cfg(test)]
use crate::writer_schema::{bytes_value, number};
#[cfg(test)]
use serde_json::Map;
use serde_json::Value;
use std::collections::BTreeMap;

/// Schema decoders prepared before records enter the evaluation loop.
#[derive(Clone, Debug, Default)]
pub struct PayloadDecoders {
    schemas: BTreeMap<u32, WriterSchema>,
}

impl PayloadDecoders {
    pub fn compile(filter: &ConsumerFilter, schemas: &[SchemaDef]) -> Result<Self, InvalidError> {
        let mut prepared = BTreeMap::new();
        for id in &filter.schema_refs {
            let schema = schemas
                .iter()
                .find(|schema| schema.id == *id)
                .ok_or_else(|| InvalidError::new(format!("writer schema {id} was not supplied")))?;
            let family_matches = matches!(
                (&schema.source, filter.codec),
                (SchemaSource::Avro { .. }, FilterCodec::Avro)
                    | (SchemaSource::Protobuf { .. }, FilterCodec::Protobuf)
            );
            if !family_matches {
                return Err(InvalidError::new(format!(
                    "schema {id} does not match codec {}",
                    filter.codec
                )));
            }
            let decoder = WriterSchema::compile(&schema.source)
                .map_err(|error| InvalidError::new(format!("schema {id}: {error}")))?;
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
            WriterSchema::Avro(schema) if codec == FilterCodec::Avro => {
                schema.decode_selected(record.payload, limits, paths)
            }
            WriterSchema::Protobuf(descriptor) if codec == FilterCodec::Protobuf => {
                decode_protobuf(descriptor, record.payload, limits)
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
