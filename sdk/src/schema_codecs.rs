use crate::error::LaserError;
use crate::query::{SchemaDef, SchemaSource};
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use serde::Serialize;
use std::sync::Arc;

/// A registered writer schema compiled for client-side use: encode Avro bodies
/// from serde values and validate payloads against the definition BEFORE
/// publishing, with the same decode semantics LaserData Cloud's projector applies.
/// Without this, the first feedback about a bad schema-first payload is a
/// managed-side warning the producer cannot see.
///
/// Compile once per schema and reuse the result across publishes (parsing is
/// the expensive part):
///
/// ```no_run
/// # use laser_sdk::prelude::*;
/// # use laser_sdk::schema_codecs::CompiledSchema;
/// # use serde::Serialize;
/// # #[derive(Serialize)] struct Reading { host: String, cpu: i64 }
/// # async fn run(laser: &Laser, reading: Reading) -> Result<(), LaserError> {
/// let info = laser.schemas().get(7).await?.expect("schema registered");
/// let compiled = CompiledSchema::compile(&info.schema)?;
/// let readings = laser.topic("readings");
/// readings.publish()
///     .index("host", &reading.host)
///     .avro(&compiled, 7, &reading)?
///     .send().await?;
/// # Ok(()) }
/// ```
#[derive(Clone)]
pub enum CompiledSchema {
    /// A parsed Avro writer schema.
    Avro(apache_avro::Schema),
    /// A resolved Protobuf message descriptor.
    Protobuf(MessageDescriptor),
    /// A compiled JSON Schema validator (draft 2020-12) for the
    /// self-describing codecs.
    Json(Arc<jsonschema::Validator>),
}

impl std::fmt::Debug for CompiledSchema {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Avro(schema) => formatter.debug_tuple("Avro").field(schema).finish(),
            Self::Protobuf(descriptor) => {
                formatter.debug_tuple("Protobuf").field(descriptor).finish()
            }
            Self::Json(_) => formatter.debug_tuple("Json").finish(),
        }
    }
}

impl CompiledSchema {
    /// Parse a registered [`SchemaDef`] into its compiled form. Fails with
    /// [`LaserError::Invalid`] when the definition does not parse (malformed
    /// Avro JSON, undecodable descriptor set, or a missing message type) -
    /// the same definitions LaserData Cloud would skip with a warning.
    pub fn compile(def: &SchemaDef) -> Result<Self, LaserError> {
        match &def.source {
            SchemaSource::Avro { schema } => {
                let parsed = apache_avro::Schema::parse_str(schema).map_err(|error| {
                    LaserError::Invalid(format!("schema {}: unparseable Avro: {error}", def.id))
                })?;
                Ok(Self::Avro(parsed))
            }
            SchemaSource::Protobuf {
                descriptor_set,
                message_type,
            } => {
                let pool = DescriptorPool::decode(descriptor_set.as_slice()).map_err(|error| {
                    LaserError::Invalid(format!(
                        "schema {}: undecodable Protobuf descriptor set: {error}",
                        def.id
                    ))
                })?;
                let descriptor = pool.get_message_by_name(message_type).ok_or_else(|| {
                    LaserError::Invalid(format!(
                        "schema {}: descriptor set has no message `{message_type}`",
                        def.id
                    ))
                })?;
                Ok(Self::Protobuf(descriptor))
            }
            SchemaSource::JsonSchema { schema } => {
                let value: serde_json::Value = serde_json::from_str(schema).map_err(|error| {
                    LaserError::Invalid(format!(
                        "schema {}: JSON Schema is not valid JSON: {error}",
                        def.id
                    ))
                })?;
                let validator = jsonschema::validator_for(&value).map_err(|error| {
                    LaserError::Invalid(format!(
                        "schema {}: JSON Schema does not compile: {error}",
                        def.id
                    ))
                })?;
                Ok(Self::Json(Arc::new(validator)))
            }
            _ => Err(LaserError::Invalid(format!(
                "schema {}: unknown schema source kind",
                def.id
            ))),
        }
    }

    /// Whether `payload` decodes (Avro, Protobuf) or, for a JSON Schema,
    /// parses as JSON text and passes validation, exactly the check
    /// LaserData Cloud's projector runs before indexing body fields. `false`
    /// means the record would fall back to header-only `agdx.idx.*` extraction. For
    /// non-JSON self-describing payloads (MessagePack, CBOR, BSON), decode
    /// them yourself and use [`CompiledSchema::validate_value`].
    pub fn validate(&self, payload: &[u8]) -> bool {
        self.decode(payload).is_ok()
    }

    /// Validate an already-decoded payload against a JSON Schema, the check
    /// LaserData Cloud runs on a self-describing record stamping `agdx.sid`. Avro and
    /// Protobuf schemas return `false`: stamping their id on a
    /// self-describing record is itself the mismatch LaserData Cloud reports.
    pub fn validate_value(&self, value: &serde_json::Value) -> bool {
        match self {
            Self::Json(validator) => validator.is_valid(value),
            Self::Avro(_) | Self::Protobuf(_) => false,
        }
    }

    /// Decode `payload` under this schema, lowered to a `serde_json::Value` -
    /// the same model LaserData Cloud extracts indexed fields from. Use it to check
    /// which JSON pointers a projection's extraction plan would resolve.
    pub fn decode(&self, payload: &[u8]) -> Result<serde_json::Value, LaserError> {
        match self {
            Self::Avro(schema) => {
                let mut cursor = payload;
                let value = apache_avro::reader::datum::GenericDatumReader::builder(schema)
                    .build()
                    .and_then(|reader| reader.read_value(&mut cursor))
                    .map_err(|error| {
                        LaserError::Codec(format!("payload does not decode as Avro: {error}"))
                    })?;
                apache_avro::from_value::<serde_json::Value>(&bytes_as_arrays(value)).map_err(
                    |error| {
                        LaserError::Codec(format!("Avro value does not lower to JSON: {error}"))
                    },
                )
            }
            Self::Protobuf(descriptor) => {
                let message =
                    DynamicMessage::decode(descriptor.clone(), payload).map_err(|error| {
                        LaserError::Codec(format!("payload does not decode as Protobuf: {error}"))
                    })?;
                serde_json::to_value(&message).map_err(|error| {
                    LaserError::Codec(format!("Protobuf message does not lower to JSON: {error}"))
                })
            }
            Self::Json(validator) => {
                let value: serde_json::Value =
                    serde_json::from_slice(payload).map_err(|error| {
                        LaserError::Codec(format!("payload does not parse as JSON: {error}"))
                    })?;
                if !validator.is_valid(&value) {
                    return Err(LaserError::Codec(
                        "payload fails its JSON Schema".to_owned(),
                    ));
                }
                Ok(value)
            }
        }
    }

    /// Encode a serde value as a raw Avro datum (single-object encoding, no
    /// container header), exactly the bytes a producer stamps alongside
    /// `agdx.sid`. Avro schemas only. A Protobuf schema returns
    /// [`LaserError::Invalid`] (encode Protobuf bodies with `prost` and ship
    /// them via `.raw_bytes(bytes, ContentType::Protobuf)`).
    pub fn encode_avro<T: Serialize>(&self, body: &T) -> Result<Vec<u8>, LaserError> {
        let Self::Avro(schema) = self else {
            return Err(LaserError::Invalid(
                "encode_avro requires an Avro schema".to_owned(),
            ));
        };
        // Lower through JSON, not serde-direct: apache-avro's serde treats a
        // `u64` (every non-negative integer arriving as a `serde_json::Value`)
        // as its 8-byte Fixed marker type and refuses a `long` schema, while
        // the JSON conversion maps integers to Int/Long.
        let json = serde_json::to_value(body)
            .map_err(|error| LaserError::Codec(format!("body does not lower to JSON: {error}")))?;
        let resolved = apache_avro::types::Value::try_from(json)
            .and_then(|value| arrays_as_bytes(value, schema).resolve(schema))
            .map_err(|error| {
                LaserError::Codec(format!("body does not match the Avro schema: {error}"))
            })?;
        apache_avro::writer::datum::GenericDatumWriter::builder(schema)
            .build()
            .and_then(|writer| writer.write_value_to_vec(resolved))
            .map_err(|error| LaserError::Codec(format!("Avro datum encode failed: {error}")))
    }
}

// The inverse of `bytes_as_arrays`, guided by the schema: a JSON array of byte
// values in a `bytes` or `fixed` position becomes Avro bytes, which also
// resolves into `fixed`. Arrays anywhere else stay arrays.
fn arrays_as_bytes(
    value: apache_avro::types::Value,
    schema: &apache_avro::Schema,
) -> apache_avro::types::Value {
    use apache_avro::Schema;
    use apache_avro::types::Value;
    match (schema, value) {
        (Schema::Bytes | Schema::Fixed(_), Value::Array(items)) => {
            let bytes: Option<Vec<u8>> = items
                .iter()
                .map(|item| match item {
                    Value::Int(byte) => u8::try_from(*byte).ok(),
                    Value::Long(byte) => u8::try_from(*byte).ok(),
                    _ => None,
                })
                .collect();
            bytes.map_or(Value::Array(items), Value::Bytes)
        }
        (Schema::Array(array), Value::Array(items)) => Value::Array(
            items
                .into_iter()
                .map(|item| arrays_as_bytes(item, &array.items))
                .collect(),
        ),
        (Schema::Map(map), Value::Map(entries)) => Value::Map(
            entries
                .into_iter()
                .map(|(key, item)| (key, arrays_as_bytes(item, &map.types)))
                .collect(),
        ),
        (Schema::Record(record), Value::Map(entries)) => Value::Map(
            entries
                .into_iter()
                .map(|(key, item)| {
                    let item = match record.fields.iter().find(|field| field.name == key) {
                        Some(field) => arrays_as_bytes(item, &field.schema),
                        None => item,
                    };
                    (key, item)
                })
                .collect(),
        ),
        (Schema::Union(union), value) => {
            let variant = union.variants().iter().find(|variant| {
                matches!(
                    (variant, &value),
                    (Schema::Bytes | Schema::Fixed(_), Value::Array(_))
                        | (Schema::Record(_) | Schema::Map(_), Value::Map(_))
                )
            });
            let has_array = union
                .variants()
                .iter()
                .any(|variant| matches!(variant, Schema::Array(_)));
            match variant {
                Some(variant) if !(has_array && matches!(value, Value::Array(_))) => {
                    arrays_as_bytes(value, variant)
                }
                _ => value,
            }
        }
        (_, value) => value,
    }
}

// JSON has no byte type, so an Avro `bytes` or `fixed` value lowers to an
// array of byte values, the same shape `encode_avro` accepts for that field.
fn bytes_as_arrays(value: apache_avro::types::Value) -> apache_avro::types::Value {
    use apache_avro::types::Value;
    match value {
        Value::Bytes(bytes) | Value::Fixed(_, bytes) => Value::Array(
            bytes
                .into_iter()
                .map(|byte| Value::Int(i32::from(byte)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.into_iter().map(bytes_as_arrays).collect()),
        Value::Map(entries) => Value::Map(
            entries
                .into_iter()
                .map(|(key, item)| (key, bytes_as_arrays(item)))
                .collect(),
        ),
        Value::Record(fields) => Value::Record(
            fields
                .into_iter()
                .map(|(name, item)| (name, bytes_as_arrays(item)))
                .collect(),
        ),
        Value::Union(index, item) => Value::Union(index, Box::new(bytes_as_arrays(*item))),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_bytes_and_fixed_fields_when_encoded_then_should_decode_back() {
        let def = SchemaDef {
            id: 8,
            source: SchemaSource::Avro {
                schema: r#"{
                    "type":"record","name":"Frame",
                    "fields":[
                        {"name":"body","type":"bytes"},
                        {"name":"digest","type":{"type":"fixed","name":"Digest","size":2}},
                        {"name":"tag","type":["null","bytes"]}
                    ]
                }"#
                .to_owned(),
            },
            name: None,
            version: None,
        };
        let compiled = CompiledSchema::compile(&def).expect("schema compiles");
        let body = serde_json::json!({"body": [1, 2, 255], "digest": [7, 9], "tag": [4]});
        let datum = compiled.encode_avro(&body).expect("body encodes");
        assert!(compiled.validate(&datum), "own encoding validates");
        assert_eq!(compiled.decode(&datum).expect("datum decodes"), body);
    }

    const READING_AVRO_SCHEMA: &str = r#"{
        "type":"record","name":"Reading",
        "fields":[
            {"name":"host","type":"string"},
            {"name":"cpu","type":"long"}
        ]
    }"#;

    #[derive(Serialize)]
    struct Reading {
        host: String,
        cpu: i64,
    }

    fn avro_def() -> SchemaDef {
        SchemaDef {
            id: 7,
            source: SchemaSource::Avro {
                schema: READING_AVRO_SCHEMA.to_owned(),
            },
            name: None,
            version: None,
        }
    }

    #[test]
    fn given_an_avro_def_when_encoded_then_should_decode_and_validate_back() {
        let compiled = CompiledSchema::compile(&avro_def()).expect("schema compiles");
        let reading = Reading {
            host: "node-7".to_owned(),
            cpu: 42,
        };
        let datum = compiled.encode_avro(&reading).expect("body encodes");
        assert!(compiled.validate(&datum), "own encoding validates");
        let value = compiled.decode(&datum).expect("datum decodes");
        assert_eq!(
            value.pointer("/host").and_then(|v| v.as_str()),
            Some("node-7")
        );
        assert_eq!(value.pointer("/cpu").and_then(|v| v.as_i64()), Some(42));
    }

    #[test]
    fn given_a_mismatched_body_when_encoded_then_should_error() {
        #[derive(Serialize)]
        struct Wrong {
            unrelated: bool,
        }
        let compiled = CompiledSchema::compile(&avro_def()).expect("schema compiles");
        let result = compiled.encode_avro(&Wrong { unrelated: true });
        assert!(matches!(result, Err(LaserError::Codec(_))));
    }

    #[test]
    fn given_garbage_bytes_when_validated_then_should_be_false() {
        let compiled = CompiledSchema::compile(&avro_def()).expect("schema compiles");
        assert!(!compiled.validate(b""));
    }

    #[test]
    fn given_an_unparseable_avro_def_when_compiled_then_should_be_invalid() {
        let def = SchemaDef {
            id: 9,
            source: SchemaSource::Avro {
                schema: "{not valid".to_owned(),
            },
            name: None,
            version: None,
        };
        assert!(matches!(
            CompiledSchema::compile(&def),
            Err(LaserError::Invalid(_))
        ));
    }

    #[test]
    fn given_a_json_schema_def_when_compiled_then_should_validate_and_decode_json() {
        let def = SchemaDef {
            id: 5,
            source: SchemaSource::JsonSchema {
                schema: r#"{
                    "type":"object",
                    "required":["host","cpu"],
                    "properties":{
                        "host":{"type":"string"},
                        "cpu":{"type":"integer","minimum":0}
                    }
                }"#
                .to_owned(),
            },
            name: None,
            version: None,
        };
        let compiled = CompiledSchema::compile(&def).expect("schema compiles");
        assert!(compiled.validate(br#"{"host":"node-7","cpu":42}"#));
        assert!(!compiled.validate(br#"{"host":"node-7","cpu":"42"}"#));
        assert!(!compiled.validate(b"not json"));
        assert!(compiled.validate_value(&serde_json::json!({"host":"a","cpu":1})));
        assert!(!compiled.validate_value(&serde_json::json!({"cpu":1})));
        let value = compiled
            .decode(br#"{"host":"node-7","cpu":42}"#)
            .expect("valid payload decodes");
        assert_eq!(
            value.pointer("/host").and_then(|v| v.as_str()),
            Some("node-7")
        );
        assert!(matches!(
            compiled.decode(br#"{"cpu":42}"#),
            Err(LaserError::Codec(_))
        ));
        assert!(matches!(
            compiled.encode_avro(&serde_json::json!({})),
            Err(LaserError::Invalid(_))
        ));
    }

    #[test]
    fn given_an_uncompilable_json_schema_def_when_compiled_then_should_be_invalid() {
        let def = SchemaDef {
            id: 6,
            source: SchemaSource::JsonSchema {
                schema: "{not json".to_owned(),
            },
            name: None,
            version: None,
        };
        assert!(matches!(
            CompiledSchema::compile(&def),
            Err(LaserError::Invalid(_))
        ));
    }

    #[test]
    fn given_an_avro_def_when_value_validated_then_should_report_family_mismatch() {
        let compiled = CompiledSchema::compile(&avro_def()).expect("schema compiles");
        assert!(!compiled.validate_value(&serde_json::json!({"host":"a","cpu":1})));
    }

    #[test]
    fn given_a_protobuf_def_when_compiled_then_should_validate_real_messages() {
        use prost::Message;
        let dir = tempfile::tempdir().expect("temp dir");
        let proto = dir.path().join("reading.proto");
        std::fs::write(
            &proto,
            "syntax = \"proto3\";\npackage fleet;\nmessage Reading { string host = 1; int64 cpu = 2; }\n",
        )
        .expect("proto written");
        let descriptor_set = protox::compile([&proto], [dir.path()])
            .expect("proto compiles")
            .encode_to_vec();
        let def = SchemaDef {
            id: 3,
            source: SchemaSource::Protobuf {
                descriptor_set: descriptor_set.clone(),
                message_type: "fleet.Reading".to_owned(),
            },
            name: None,
            version: None,
        };
        let compiled = CompiledSchema::compile(&def).expect("schema compiles");

        let pool = DescriptorPool::decode(descriptor_set.as_slice()).expect("pool decodes");
        let descriptor = pool.get_message_by_name("fleet.Reading").expect("message");
        let mut message = DynamicMessage::new(descriptor);
        message.set_field_by_name("host", prost_reflect::Value::String("node-7".into()));
        message.set_field_by_name("cpu", prost_reflect::Value::I64(42));
        let payload = message.encode_to_vec();

        assert!(compiled.validate(&payload));
        assert!(!compiled.validate(b"\xff\xff\xff"));
        let value = compiled.decode(&payload).expect("decodes");
        assert_eq!(
            value.pointer("/host").and_then(|v| v.as_str()),
            Some("node-7")
        );
        let encode = compiled.encode_avro(&serde_json::json!({}));
        assert!(matches!(encode, Err(LaserError::Invalid(_))));
    }
}
