use crate::error::LaserError;
use crate::query::{SchemaDef, SchemaSource};
use laser_wire::validate::Validate;
use laser_wire::writer_schema::{
    AvroWriterSchema, RECORD_DECODE_LIMITS, compile_protobuf, decode_protobuf,
};
use prost_reflect::MessageDescriptor;
use serde::Serialize;
use std::sync::Arc;

/// A registered writer schema compiled for client-side use: encode Avro bodies
/// from serde values and validate payloads against the definition BEFORE
/// publishing, with the decode LaserData Cloud's projector, browse decode and
/// consumer filters apply. Without this, the first feedback about a bad
/// schema-first payload is a managed-side warning the producer cannot see.
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
    /// An Avro writer schema without logical annotations, so values are physical.
    Avro(AvroWriterSchema),
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
    /// [`LaserError::Invalid`] for a definition the registry refuses: malformed
    /// Avro JSON, an undecodable descriptor set, a missing message type, a JSON
    /// Schema of another draft than 2020-12, or one past the 1 MiB and depth 64
    /// bounds.
    pub fn compile(def: &SchemaDef) -> Result<Self, LaserError> {
        let invalid = |error: laser_wire::error::InvalidError| {
            LaserError::Invalid(format!("schema {}: {error}", def.id))
        };
        def.source.validate().map_err(invalid)?;
        match &def.source {
            SchemaSource::Avro { schema } => AvroWriterSchema::compile(schema)
                .map(Self::Avro)
                .map_err(invalid),
            SchemaSource::Protobuf {
                descriptor_set,
                message_type,
            } => compile_protobuf(descriptor_set, message_type)
                .map(Self::Protobuf)
                .map_err(invalid),
            SchemaSource::JsonSchema { schema } => {
                let value: serde_json::Value = serde_json::from_str(schema).map_err(|error| {
                    LaserError::Invalid(format!(
                        "schema {}: JSON Schema is not valid JSON: {error}",
                        def.id
                    ))
                })?;
                let validator = jsonschema::draft202012::new(&value).map_err(|error| {
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
    /// means the record breaks its schema contract. For non-JSON
    /// self-describing payloads (MessagePack, CBOR, BSON), decode them yourself
    /// and use [`CompiledSchema::validate_value`].
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

    /// Decode `payload` under this schema into the shared model LaserData
    /// Cloud extracts indexed fields from: schema field names, numeric enums,
    /// exact 64-bit integers, bytes as integer arrays, proto3 defaults present
    /// and Avro logical types as their physical values. Use it to check which
    /// JSON pointers a projection's extraction plan resolves.
    pub fn decode(&self, payload: &[u8]) -> Result<serde_json::Value, LaserError> {
        match self {
            Self::Avro(schema) => schema
                .decode(payload, &RECORD_DECODE_LIMITS)
                .map_err(|reason| {
                    LaserError::Codec(format!("payload does not decode as Avro: {reason}"))
                }),
            Self::Protobuf(descriptor) => {
                decode_protobuf(descriptor, payload, &RECORD_DECODE_LIMITS).map_err(|reason| {
                    LaserError::Codec(format!("payload does not decode as Protobuf: {reason}"))
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

    /// Encode a serde value as a bare Avro datum (no single-object or
    /// container header), exactly the bytes a producer stamps alongside
    /// `agdx.sid`. Values are physical: a decimal is its unscaled bytes, a
    /// timestamp its long. Avro schemas only. A Protobuf schema returns
    /// [`LaserError::Invalid`] (encode Protobuf bodies with `prost` and ship
    /// them via `.raw_bytes(bytes, ContentType::Protobuf)`).
    pub fn encode_avro<T: Serialize>(&self, body: &T) -> Result<Vec<u8>, LaserError> {
        let Self::Avro(schema) = self else {
            return Err(LaserError::Invalid(
                "encode_avro requires an Avro schema".to_owned(),
            ));
        };
        let schema = schema.schema();
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

// The inverse of the decode lowering, guided by the schema: a JSON array of
// byte values in a `bytes` or `fixed` position becomes Avro bytes, which also
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

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::types::Value as Avro;
    use laser_wire::filter::FaultReason;
    use laser_wire::writer_schema::AvroWriterSchema;
    use prost::Message;
    use prost_reflect::Value as Proto;
    use prost_reflect::{DescriptorPool, DynamicMessage};
    use serde::Deserialize;
    use serde_json::json;
    use std::collections::HashMap;

    const WRITER_SCHEMA_CASES_PATH: &str = "../wire/fixtures/writer_schema_cases.json";

    #[test]
    fn given_the_writer_schema_case_definitions_when_encoded_then_should_match_the_shared_corpus() {
        let corpus = writer_schema_corpus();
        let mut encoded = serde_json::to_string_pretty(&corpus).expect("the corpus encodes");
        encoded.push('\n');
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(WRITER_SCHEMA_CASES_PATH);
        if std::env::var_os("AGDX_WIRE_FIXTURES_REGEN").is_some() {
            std::fs::write(&path, &encoded).expect("the corpus is written");
        }
        assert_eq!(
            std::fs::read_to_string(&path).expect("the corpus is readable"),
            encoded,
            "regenerate with AGDX_WIRE_FIXTURES_REGEN=1"
        );
    }

    #[test]
    fn given_the_writer_schema_corpus_when_decoded_then_should_match_every_expected_value() {
        let corpus: WriterSchemaCorpus =
            serde_json::from_str(include_str!("../../wire/fixtures/writer_schema_cases.json"))
                .expect("the corpus decodes");
        let mut failures = Vec::new();
        for case in &corpus.cases {
            let definition = corpus
                .schemas
                .iter()
                .find(|definition| definition.id == case.schema)
                .unwrap_or_else(|| panic!("{} names no corpus schema", case.name));
            let compiled = CompiledSchema::compile(definition);
            if case.refused {
                if !matches!(compiled, Err(LaserError::Invalid(_))) {
                    failures.push(format!("{}: compiled", case.name));
                }
                continue;
            }
            let compiled = match compiled {
                Ok(compiled) => compiled,
                Err(error) => {
                    failures.push(format!("{}: {error}", case.name));
                    continue;
                }
            };
            let payload = hex_bytes(case.payload_hex.as_deref().unwrap_or_default());
            match (compiled.decode(&payload), &case.expected) {
                (Ok(value), Some(expected)) if &value == expected => {}
                (Err(LaserError::Codec(_)), None) => {}
                (outcome, _) => failures.push(format!("{}: {outcome:?}", case.name)),
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
    }

    #[test]
    fn given_a_protobuf_reading_with_an_exact_i64_when_decoded_into_a_struct_then_should_fit_it() {
        #[derive(Debug, PartialEq, Deserialize)]
        struct Reading {
            host: String,
            cpu_usage: i64,
            mode: i32,
        }
        let definition = SchemaDef {
            id: 1,
            source: corpus_protobuf_source(),
            name: None,
            version: None,
        };
        let compiled = CompiledSchema::compile(&definition).expect("the schema compiles");
        let payload = corpus_reading(&[
            ("host", Proto::String("node-7".to_owned())),
            ("cpu_usage", Proto::I64(-9_007_199_254_740_993)),
            ("mode", Proto::EnumNumber(2)),
        ]);
        let reading: Reading = serde_json::from_value(compiled.decode(&payload).expect("decodes"))
            .expect("the lowering fits the struct");
        assert_eq!(
            reading,
            Reading {
                host: "node-7".to_owned(),
                cpu_usage: -9_007_199_254_740_993,
                mode: 2,
            }
        );
    }

    #[test]
    fn given_invalid_utf8_inside_a_json_string_when_decoded_then_should_reject_the_payload() {
        let compiled = CompiledSchema::compile(&SchemaDef {
            id: 1,
            source: SchemaSource::JsonSchema {
                schema: "{\"type\":\"string\"}".to_owned(),
            },
            name: None,
            version: None,
        })
        .expect("the schema compiles");
        assert!(matches!(
            compiled.decode(&[34, 255, 34]),
            Err(LaserError::Codec(_))
        ));
        assert!(!compiled.validate(&[34, 255, 34]));
    }

    #[test]
    fn given_an_avro_datum_with_trailing_bytes_when_decoded_then_should_reject_the_payload() {
        let compiled = CompiledSchema::compile(&SchemaDef {
            id: 1,
            source: SchemaSource::Avro {
                schema: "\"long\"".to_owned(),
            },
            name: None,
            version: None,
        })
        .expect("the schema compiles");
        assert_eq!(
            compiled.decode(&[14]).expect("one datum decodes"),
            serde_json::Value::from(7)
        );
        assert!(matches!(
            compiled.decode(&[14, 0]),
            Err(LaserError::Codec(_))
        ));
        assert!(!compiled.validate(&[14, 0]));
    }

    #[derive(serde::Deserialize)]
    struct ValidationCase {
        name: String,
        schema: serde_json::Value,
        accepted: Vec<serde_json::Value>,
        rejected: Vec<serde_json::Value>,
    }

    #[test]
    fn given_valid_json_schema_forms_when_compiled_then_should_match_the_shared_validation_cases() {
        let cases: Vec<ValidationCase> = serde_json::from_str(include_str!(
            "../../wire/fixtures/schema_validation_cases.json"
        ))
        .expect("the validation cases decode");
        for case in cases {
            let definition = SchemaDef {
                id: 1,
                source: SchemaSource::JsonSchema {
                    schema: case.schema.to_string(),
                },
                name: None,
                version: None,
            };
            let compiled = CompiledSchema::compile(&definition).expect("the valid schema compiles");
            for value in case.accepted {
                assert!(
                    compiled.validate_value(&value),
                    "{} rejected {value}",
                    case.name
                );
            }
            for value in case.rejected {
                assert!(
                    !compiled.validate_value(&value),
                    "{} accepted {value}",
                    case.name
                );
            }
        }
    }

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

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct WriterSchemaCorpus {
        schemas: Vec<SchemaDef>,
        cases: Vec<WriterSchemaCase>,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct WriterSchemaCase {
        name: String,
        schema: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload_hex: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expected: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        fault: Option<FaultReason>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        refused: bool,
    }

    const CORPUS_PROTO: &str = r#"syntax = "proto3";
package corpus;

import "google/protobuf/timestamp.proto";
import "google/protobuf/wrappers.proto";

enum Mode {
  MODE_UNSPECIFIED = 0;
  MODE_WARM = 2;
}

message Inner {
  string label = 1;
}

message Reading {
  string host = 1;
  int64 cpu_usage = 2;
  uint64 total = 3;
  sint32 delta = 4;
  bool active = 5;
  double ratio = 6;
  bytes digest = 7;
  Mode mode = 8;
  repeated int32 samples = 9;
  map<string, int64> counters = 10;
  map<int64, string> names_by_id = 11;
  oneof choice {
    string text = 12;
    Inner inner = 13;
  }
  optional int32 maybe = 14;
  google.protobuf.Timestamp observed_at = 15;
  google.protobuf.Int64Value boxed = 16;
  fixed64 checksum = 17;
  float level = 18;
}
"#;

    fn writer_schema_corpus() -> WriterSchemaCorpus {
        let primitives = r#"{"type":"record","name":"Primitives","fields":[{"name":"flag","type":"boolean"},{"name":"small","type":"int"},{"name":"large","type":"long"},{"name":"ratio","type":"float"},{"name":"precise","type":"double"},{"name":"label","type":"string"},{"name":"nothing","type":"null"}]}"#;
        let frame = r#"{"type":"record","name":"Frame","fields":[{"name":"body","type":"bytes"},{"name":"digest","type":{"type":"fixed","name":"Digest","size":2}}]}"#;
        let logical = r#"{"type":"record","name":"Logical","fields":[{"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":8,"scale":2}},{"name":"fixed_price","type":{"type":"fixed","name":"Price","size":4,"logicalType":"decimal","precision":6,"scale":1}},{"name":"span","type":{"type":"fixed","name":"Span","size":12,"logicalType":"duration"}},{"name":"id","type":{"type":"string","logicalType":"uuid"}},{"name":"day","type":{"type":"int","logicalType":"date"}},{"name":"clock","type":{"type":"int","logicalType":"time-millis"}},{"name":"at_micros","type":{"type":"long","logicalType":"timestamp-micros"}},{"name":"at_nanos","type":{"type":"long","logicalType":"timestamp-nanos"}}]}"#;
        let unions = r#"{"type":"record","name":"Unions","fields":[{"name":"count","type":["null","int","long"]},{"name":"label","type":["null","string"]},{"name":"body","type":[{"type":"record","name":"Alpha","fields":[{"name":"x","type":"long"}]},{"type":"record","name":"Beta","fields":[{"name":"y","type":"string"}]}]}]}"#;
        let collections = r#"{"type":"record","name":"Collections","fields":[{"name":"state","type":{"type":"enum","name":"State","symbols":["ready","warm"]}},{"name":"samples","type":{"type":"array","items":"int"}},{"name":"totals","type":{"type":"map","values":"long"}}]}"#;
        let deep = format!(
            "{}\"long\"{}",
            r#"{"type":"array","items":"#.repeat(65),
            "}".repeat(65)
        );
        let reading = corpus_reading(&[
            ("host", Proto::String("node-7".to_owned())),
            ("cpu_usage", Proto::I64(-9_007_199_254_740_993)),
            ("total", Proto::U64(u64::MAX)),
            ("delta", Proto::I32(-3)),
            ("active", Proto::Bool(true)),
            ("ratio", Proto::F64(0.75)),
            ("digest", Proto::Bytes(vec![0, 255].into())),
            ("mode", Proto::EnumNumber(2)),
            (
                "samples",
                Proto::List(vec![Proto::I32(1), Proto::I32(2), Proto::I32(3)]),
            ),
            (
                "counters",
                Proto::Map(HashMap::from([(
                    prost_reflect::MapKey::String("a".to_owned()),
                    Proto::I64(9_007_199_254_740_993),
                )])),
            ),
            (
                "names_by_id",
                Proto::Map(HashMap::from([(
                    prost_reflect::MapKey::I64(-5),
                    Proto::String("minus five".to_owned()),
                )])),
            ),
            ("text", Proto::String("hello".to_owned())),
            ("maybe", Proto::I32(0)),
            ("checksum", Proto::U64(1 << 40)),
            ("level", Proto::F32(0.5)),
        ]);
        let reading = with_messages(
            &reading,
            &[
                (
                    "observed_at",
                    &[
                        ("seconds", Proto::I64(1_700_000_000)),
                        ("nanos", Proto::I32(5)),
                    ],
                ),
                ("boxed", &[("value", Proto::I64(42))]),
            ],
        );
        let inner = with_messages(
            &corpus_reading(&[("host", Proto::String("h".to_owned()))]),
            &[("inner", &[("label", Proto::String("x".to_owned()))])],
        );
        let defaults = json!({
            "host": "", "cpu_usage": 0, "total": 0, "delta": 0, "active": false,
            "ratio": 0.0, "digest": [], "mode": 0, "samples": [], "counters": {},
            "names_by_id": {}, "checksum": 0, "level": 0.0
        });
        let mut with_inner = defaults.clone();
        with_inner["host"] = json!("h");
        with_inner["inner"] = json!({"label": "x"});
        let json_schema = r#"{"type":"object","required":["id"],"properties":{"id":{"type":"integer"},"tags":{"type":"array","items":{"type":"string"}}}}"#;
        let protobuf = corpus_protobuf_source();
        let missing_message = match &protobuf {
            SchemaSource::Protobuf { descriptor_set, .. } => SchemaSource::Protobuf {
                descriptor_set: descriptor_set.clone(),
                message_type: "corpus.Missing".to_owned(),
            },
            other => other.clone(),
        };
        let sources = [
            avro(primitives),
            avro(frame),
            avro(logical),
            avro(unions),
            avro(collections),
            avro("\"long\""),
            avro(&deep),
            avro("\"decimal128\""),
            protobuf,
            missing_message,
            json(json_schema),
            json(r#"{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"integer"}"#),
            json(r#"{"$schema":"http://json-schema.org/draft-07/schema#","type":"object"}"#),
        ];
        let schemas = sources
            .into_iter()
            .zip(1..)
            .map(|(source, id)| SchemaDef {
                id,
                source,
                name: None,
                version: None,
            })
            .collect();
        let cases = vec![
            decoded(
                "avro primitives keep exact longs",
                1,
                avro_datum(
                    primitives,
                    Avro::Record(vec![
                        ("flag".to_owned(), Avro::Boolean(true)),
                        ("small".to_owned(), Avro::Int(-7)),
                        ("large".to_owned(), Avro::Long(9_007_199_254_740_993)),
                        ("ratio".to_owned(), Avro::Float(1.5)),
                        ("precise".to_owned(), Avro::Double(-0.25)),
                        ("label".to_owned(), Avro::String("λ-7".to_owned())),
                        ("nothing".to_owned(), Avro::Null),
                    ]),
                ),
                json!({"flag": true, "small": -7, "large": 9_007_199_254_740_993_i64, "ratio": 1.5,
                    "precise": -0.25, "label": "λ-7", "nothing": null}),
            ),
            decoded(
                "avro bytes and fixed become integer arrays",
                2,
                avro_datum(
                    frame,
                    Avro::Record(vec![
                        ("body".to_owned(), Avro::Bytes(vec![0, 128, 255])),
                        ("digest".to_owned(), Avro::Fixed(2, vec![7, 9])),
                    ]),
                ),
                json!({"body": [0, 128, 255], "digest": [7, 9]}),
            ),
            decoded(
                "avro logical types decode as their physical values",
                3,
                avro_datum(
                    logical,
                    Avro::Record(vec![
                        ("price".to_owned(), Avro::Bytes(vec![0x30, 0x39])),
                        ("fixed_price".to_owned(), Avro::Fixed(4, vec![0, 0, 1, 0])),
                        (
                            "span".to_owned(),
                            Avro::Fixed(12, vec![1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0]),
                        ),
                        (
                            "id".to_owned(),
                            Avro::String("6f1c4c3e-6b8f-4b8e-9c56-2a3f1d7a9b10".to_owned()),
                        ),
                        ("day".to_owned(), Avro::Int(19_000)),
                        ("clock".to_owned(), Avro::Int(3_600_000)),
                        ("at_micros".to_owned(), Avro::Long(1_700_000_000_000_000)),
                        ("at_nanos".to_owned(), Avro::Long(1_700_000_000_000_000_001)),
                    ]),
                ),
                json!({"price": [48, 57], "fixed_price": [0, 0, 1, 0],
                    "span": [1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0],
                    "id": "6f1c4c3e-6b8f-4b8e-9c56-2a3f1d7a9b10", "day": 19_000,
                    "clock": 3_600_000, "at_micros": 1_700_000_000_000_000_i64,
                    "at_nanos": 1_700_000_000_000_000_001_i64}),
            ),
            decoded(
                "avro unions decode to the branch value",
                4,
                avro_datum(
                    unions,
                    Avro::Record(vec![
                        (
                            "count".to_owned(),
                            Avro::Union(2, Box::new(Avro::Long(9_007_199_254_740_993))),
                        ),
                        ("label".to_owned(), Avro::Union(0, Box::new(Avro::Null))),
                        (
                            "body".to_owned(),
                            Avro::Union(
                                1,
                                Box::new(Avro::Record(vec![(
                                    "y".to_owned(),
                                    Avro::String("b".to_owned()),
                                )])),
                            ),
                        ),
                    ]),
                ),
                json!({"count": 9_007_199_254_740_993_i64, "label": null, "body": {"y": "b"}}),
            ),
            decoded(
                "avro enums arrays and maps",
                5,
                avro_datum(
                    collections,
                    Avro::Record(vec![
                        ("state".to_owned(), Avro::Enum(1, "warm".to_owned())),
                        (
                            "samples".to_owned(),
                            Avro::Array(vec![Avro::Int(1), Avro::Int(-2), Avro::Int(3)]),
                        ),
                        (
                            "totals".to_owned(),
                            Avro::Map(HashMap::from([(
                                "b".to_owned(),
                                Avro::Long(9_007_199_254_740_993),
                            )])),
                        ),
                    ]),
                ),
                json!({"state": "warm", "samples": [1, -2, 3],
                    "totals": {"b": 9_007_199_254_740_993_i64}}),
            ),
            faulted(
                "avro body shorter than its bytes field",
                2,
                vec![6, 0, 128],
                FaultReason::Malformed,
            ),
            faulted(
                "avro datum with trailing bytes",
                6,
                vec![14, 0],
                FaultReason::Malformed,
            ),
            refused("avro schema past the nesting bound", 7),
            refused("avro schema with an unknown type", 8),
            decoded(
                "protobuf common types",
                9,
                reading,
                json!({"host": "node-7", "cpu_usage": -9_007_199_254_740_993_i64,
                    "total": u64::MAX, "delta": -3, "active": true, "ratio": 0.75,
                    "digest": [0, 255], "mode": 2, "samples": [1, 2, 3],
                    "counters": {"a": 9_007_199_254_740_993_i64},
                    "names_by_id": {"-5": "minus five"}, "text": "hello", "maybe": 0,
                    "observed_at": {"seconds": 1_700_000_000, "nanos": 5},
                    "boxed": {"value": 42}, "checksum": 1_u64 << 40, "level": 0.5}),
            ),
            decoded(
                "protobuf empty message keeps proto3 defaults",
                9,
                Vec::new(),
                defaults,
            ),
            decoded("protobuf oneof message branch", 9, inner, with_inner),
            faulted(
                "protobuf truncated varint",
                9,
                vec![0xff, 0xff, 0xff],
                FaultReason::Malformed,
            ),
            faulted(
                "protobuf legacy group",
                9,
                vec![0x0b],
                FaultReason::Malformed,
            ),
            refused("protobuf message type missing from the descriptor set", 10),
            decoded(
                "json schema keeps exact integers",
                11,
                br#"{"id":9007199254740993,"tags":["a","b"],"ratio":0.5}"#.to_vec(),
                json!({"id": 9_007_199_254_740_993_i64, "tags": ["a", "b"], "ratio": 0.5}),
            ),
            faulted(
                "json schema refuses a payload that fails it",
                11,
                br#"{"id":"x"}"#.to_vec(),
                FaultReason::Malformed,
            ),
            decoded(
                "json schema naming draft 2020-12",
                12,
                b"7".to_vec(),
                json!(7),
            ),
            refused("json schema naming another draft", 13),
        ];
        WriterSchemaCorpus { schemas, cases }
    }

    fn decoded(
        name: &str,
        schema: u32,
        payload: Vec<u8>,
        expected: serde_json::Value,
    ) -> WriterSchemaCase {
        WriterSchemaCase {
            name: name.to_owned(),
            schema,
            payload_hex: Some(hex_text(&payload)),
            expected: Some(expected),
            fault: None,
            refused: false,
        }
    }

    fn faulted(name: &str, schema: u32, payload: Vec<u8>, fault: FaultReason) -> WriterSchemaCase {
        WriterSchemaCase {
            name: name.to_owned(),
            schema,
            payload_hex: Some(hex_text(&payload)),
            expected: None,
            fault: Some(fault),
            refused: false,
        }
    }

    fn refused(name: &str, schema: u32) -> WriterSchemaCase {
        WriterSchemaCase {
            name: name.to_owned(),
            schema,
            payload_hex: None,
            expected: None,
            fault: None,
            refused: true,
        }
    }

    fn json(schema: &str) -> SchemaSource {
        SchemaSource::JsonSchema {
            schema: schema.to_owned(),
        }
    }

    fn avro(schema: &str) -> SchemaSource {
        SchemaSource::Avro {
            schema: schema.to_owned(),
        }
    }

    fn avro_datum(schema: &str, value: Avro) -> Vec<u8> {
        let physical = AvroWriterSchema::compile(schema).expect("the corpus schema compiles");
        apache_avro::writer::datum::GenericDatumWriter::builder(physical.schema())
            .build()
            .and_then(|writer| writer.write_value_to_vec(value))
            .expect("the corpus datum encodes")
    }

    fn corpus_protobuf_source() -> SchemaSource {
        let directory = tempfile::tempdir().expect("temp dir");
        let proto = directory.path().join("corpus.proto");
        std::fs::write(&proto, CORPUS_PROTO).expect("proto written");
        SchemaSource::Protobuf {
            descriptor_set: protox::compile([&proto], [directory.path()])
                .expect("the corpus proto compiles")
                .encode_to_vec(),
            message_type: "corpus.Reading".to_owned(),
        }
    }

    fn corpus_descriptor() -> MessageDescriptor {
        let SchemaSource::Protobuf {
            descriptor_set,
            message_type,
        } = corpus_protobuf_source()
        else {
            unreachable!("the corpus source is Protobuf")
        };
        DescriptorPool::decode(descriptor_set.as_slice())
            .expect("the corpus pool decodes")
            .get_message_by_name(&message_type)
            .expect("the corpus message exists")
    }

    fn corpus_reading(fields: &[(&str, Proto)]) -> Vec<u8> {
        let mut message = DynamicMessage::new(corpus_descriptor());
        for (name, value) in fields {
            message.set_field_by_name(name, value.clone());
        }
        message.encode_to_vec()
    }

    fn with_messages(payload: &[u8], fields: &[(&str, &[(&str, Proto)])]) -> Vec<u8> {
        let descriptor = corpus_descriptor();
        let mut message =
            DynamicMessage::decode(descriptor.clone(), payload).expect("the reading decodes");
        for (name, nested) in fields {
            let field = descriptor
                .get_field_by_name(name)
                .expect("the corpus field exists");
            let prost_reflect::Kind::Message(kind) = field.kind() else {
                unreachable!("the corpus field is a message")
            };
            let mut child = DynamicMessage::new(kind);
            for (nested_name, value) in *nested {
                child.set_field_by_name(nested_name, value.clone());
            }
            message.set_field(&field, Proto::Message(child));
        }
        message.encode_to_vec()
    }

    fn hex_text(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn hex_bytes(text: &str) -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&text[index..index + 2], 16).expect("hex byte"))
            .collect()
    }
}
