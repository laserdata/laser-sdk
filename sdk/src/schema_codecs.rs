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
/// # #[derive(Serialize)] struct Order { customer: String, amount: i64 }
/// # async fn run(laser: &Laser, order: Order) -> Result<(), LaserError> {
/// let info = laser.schemas().get(7).await?.expect("schema registered");
/// let compiled = CompiledSchema::compile(&info.schema)?;
/// let orders = laser.topic("orders");
/// orders.publish()
///     .index("customer", &order.customer)
///     .avro(&compiled, 7, &order)?
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
                let value =
                    apache_avro::from_avro_datum(schema, &mut cursor, None).map_err(|error| {
                        LaserError::Codec(format!("payload does not decode as Avro: {error}"))
                    })?;
                apache_avro::from_value::<serde_json::Value>(&value).map_err(|error| {
                    LaserError::Codec(format!("Avro value does not lower to JSON: {error}"))
                })
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
        let value = apache_avro::to_value(body)
            .map_err(|error| LaserError::Codec(format!("body does not lower to Avro: {error}")))?;
        let resolved = value.resolve(schema).map_err(|error| {
            LaserError::Codec(format!("body does not match the Avro schema: {error}"))
        })?;
        apache_avro::to_avro_datum(schema, resolved)
            .map_err(|error| LaserError::Codec(format!("Avro datum encode failed: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORDER_AVRO_SCHEMA: &str = r#"{
        "type":"record","name":"Order",
        "fields":[
            {"name":"customer","type":"string"},
            {"name":"amount","type":"long"}
        ]
    }"#;

    #[derive(Serialize)]
    struct Order {
        customer: String,
        amount: i64,
    }

    fn avro_def() -> SchemaDef {
        SchemaDef {
            id: 7,
            source: SchemaSource::Avro {
                schema: ORDER_AVRO_SCHEMA.to_owned(),
            },
            name: None,
            version: None,
        }
    }

    #[test]
    fn given_an_avro_def_when_encoded_then_should_decode_and_validate_back() {
        let compiled = CompiledSchema::compile(&avro_def()).expect("schema compiles");
        let order = Order {
            customer: "alice".to_owned(),
            amount: 42,
        };
        let datum = compiled.encode_avro(&order).expect("body encodes");
        assert!(compiled.validate(&datum), "own encoding validates");
        let value = compiled.decode(&datum).expect("datum decodes");
        assert_eq!(
            value.pointer("/customer").and_then(|v| v.as_str()),
            Some("alice")
        );
        assert_eq!(value.pointer("/amount").and_then(|v| v.as_i64()), Some(42));
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
                    "required":["customer","amount"],
                    "properties":{
                        "customer":{"type":"string"},
                        "amount":{"type":"integer","minimum":0}
                    }
                }"#
                .to_owned(),
            },
            name: None,
            version: None,
        };
        let compiled = CompiledSchema::compile(&def).expect("schema compiles");
        assert!(compiled.validate(br#"{"customer":"alice","amount":42}"#));
        assert!(!compiled.validate(br#"{"customer":"alice","amount":"42"}"#));
        assert!(!compiled.validate(b"not json"));
        assert!(compiled.validate_value(&serde_json::json!({"customer":"a","amount":1})));
        assert!(!compiled.validate_value(&serde_json::json!({"amount":1})));
        let value = compiled
            .decode(br#"{"customer":"alice","amount":42}"#)
            .expect("valid payload decodes");
        assert_eq!(
            value.pointer("/customer").and_then(|v| v.as_str()),
            Some("alice")
        );
        assert!(matches!(
            compiled.decode(br#"{"amount":42}"#),
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
        assert!(!compiled.validate_value(&serde_json::json!({"customer":"a","amount":1})));
    }

    #[test]
    fn given_a_protobuf_def_when_compiled_then_should_validate_real_messages() {
        use prost::Message;
        let dir = tempfile::tempdir().expect("temp dir");
        let proto = dir.path().join("order.proto");
        std::fs::write(
            &proto,
            "syntax = \"proto3\";\npackage shop;\nmessage Order { string customer = 1; int64 amount = 2; }\n",
        )
        .expect("proto written");
        let descriptor_set = protox::compile([&proto], [dir.path()])
            .expect("proto compiles")
            .encode_to_vec();
        let def = SchemaDef {
            id: 3,
            source: SchemaSource::Protobuf {
                descriptor_set: descriptor_set.clone(),
                message_type: "shop.Order".to_owned(),
            },
            name: None,
            version: None,
        };
        let compiled = CompiledSchema::compile(&def).expect("schema compiles");

        let pool = DescriptorPool::decode(descriptor_set.as_slice()).expect("pool decodes");
        let descriptor = pool.get_message_by_name("shop.Order").expect("message");
        let mut message = DynamicMessage::new(descriptor);
        message.set_field_by_name("customer", prost_reflect::Value::String("alice".into()));
        message.set_field_by_name("amount", prost_reflect::Value::I64(42));
        let payload = message.encode_to_vec();

        assert!(compiled.validate(&payload));
        assert!(!compiled.validate(b"\xff\xff\xff"));
        let value = compiled.decode(&payload).expect("decodes");
        assert_eq!(
            value.pointer("/customer").and_then(|v| v.as_str()),
            Some("alice")
        );
        let encode = compiled.encode_avro(&serde_json::json!({}));
        assert!(matches!(encode, Err(LaserError::Invalid(_))));
    }
}
