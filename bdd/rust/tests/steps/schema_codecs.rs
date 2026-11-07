use crate::common::world::LaserWorld;
use cucumber::{given, then, when};
use laser_sdk::LaserError;
use laser_sdk::query::{SchemaDef, SchemaSource};
use laser_sdk::schema_codecs::CompiledSchema;

#[given(regex = r#"^a "([^"]+)" writer schema$"#)]
fn writer_schema(world: &mut LaserWorld, codec: String) {
    let source = match codec.as_str() {
        "avro_long" => SchemaSource::Avro {
            schema: "\"long\"".to_owned(),
        },
        "json_string" => SchemaSource::JsonSchema {
            schema: "{\"type\":\"string\"}".to_owned(),
        },
        _ => panic!("unknown schema fixture {codec}"),
    };
    world.writer_schema = Some(
        CompiledSchema::compile(&SchemaDef {
            id: 1,
            source,
            name: None,
            version: None,
        })
        .expect("the writer schema compiles"),
    );
}

#[when(regex = r#"^I decode schema payload "([^"]+)"$"#)]
fn decode_payload(world: &mut LaserWorld, payload: String) {
    let bytes: Vec<u8> = serde_json::from_str(&payload).expect("the byte array parses");
    let schema = world.writer_schema.as_ref().expect("a writer schema");
    world.writer_payload_rejected = Some(match schema.decode(&bytes) {
        Ok(_) => false,
        Err(LaserError::Codec(_)) => true,
        Err(error) => panic!("unexpected error class: {error}"),
    });
}

#[then(regex = r#"^the schema payload is (accepted|rejected)$"#)]
fn payload_outcome(world: &mut LaserWorld, outcome: String) {
    assert_eq!(world.writer_payload_rejected, Some(outcome == "rejected"));
}

#[when("I decode every case of the shared writer schema corpus")]
fn decode_writer_corpus(world: &mut LaserWorld) {
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../wire/fixtures/writer_schema_cases.json"
    ))
    .expect("the writer schema corpus decodes");
    let schemas: Vec<SchemaDef> =
        serde_json::from_value(corpus["schemas"].clone()).expect("the corpus schemas decode");
    let mut failures = Vec::new();
    for case in corpus["cases"].as_array().expect("the corpus cases") {
        let name = case["name"].as_str().expect("a case name");
        let definition = schemas
            .iter()
            .find(|definition| Some(u64::from(definition.id)) == case["schema"].as_u64())
            .expect("the case names a corpus schema");
        let refused = case["refused"].as_bool().unwrap_or(false);
        let hex = case["payload_hex"].as_str().unwrap_or_default();
        let outcome = CompiledSchema::compile(definition).map(|compiled| {
            let payload: Vec<u8> = (0..hex.len())
                .step_by(2)
                .map(|index| u8::from_str_radix(&hex[index..index + 2], 16).expect("hex byte"))
                .collect();
            compiled.decode(&payload)
        });
        let matches = match (outcome, case.get("expected")) {
            (Err(LaserError::Invalid(_)), _) => refused,
            (Ok(Ok(value)), Some(expected)) => !refused && &value == expected,
            (Ok(Err(LaserError::Codec(_))), None) => !refused,
            _ => false,
        };
        if !matches {
            failures.push(name.to_owned());
        }
    }
    world.writer_corpus_failures = Some(failures);
}

#[then("every corpus case matches its expected value")]
fn writer_corpus_matches(world: &mut LaserWorld) {
    assert_eq!(world.writer_corpus_failures, Some(Vec::new()));
}
