use laser_wire::control::{SchemaDef, SchemaSource};
use laser_wire::filter::FaultReason;
use laser_wire::validate::Validate;
use laser_wire::writer_schema::{RECORD_DECODE_LIMITS, WriterSchema};
use serde::Deserialize;

#[derive(Deserialize)]
struct WriterSchemaCorpus {
    schemas: Vec<SchemaDef>,
    cases: Vec<WriterSchemaCase>,
}

#[derive(Deserialize)]
struct WriterSchemaCase {
    name: String,
    schema: u32,
    #[serde(default)]
    payload_hex: Option<String>,
    #[serde(default)]
    expected: Option<serde_json::Value>,
    #[serde(default)]
    fault: Option<FaultReason>,
    #[serde(default)]
    refused: bool,
}

#[test]
fn given_the_writer_schema_corpus_when_decoded_then_should_match_every_value_and_fault() {
    let corpus: WriterSchemaCorpus =
        serde_json::from_str(include_str!("../fixtures/writer_schema_cases.json"))
            .expect("the corpus decodes");
    for case in corpus.cases {
        let source = &corpus
            .schemas
            .iter()
            .find(|definition| definition.id == case.schema)
            .expect("the case names a corpus schema")
            .source;
        if case.refused {
            let compiled = match source {
                SchemaSource::JsonSchema { .. } => source.validate(),
                _ => WriterSchema::compile(source).map(|_| ()),
            };
            assert!(compiled.is_err(), "{} compiled", case.name);
            continue;
        }
        if matches!(source, SchemaSource::JsonSchema { .. }) {
            source.validate().expect(&case.name);
            continue;
        }
        let compiled = WriterSchema::compile(source).expect(&case.name);
        let payload = hex_bytes(case.payload_hex.as_deref().unwrap_or_default());
        let outcome = compiled.decode(&payload, &RECORD_DECODE_LIMITS);
        match (case.expected, case.fault) {
            (Some(expected), None) => assert_eq!(outcome, Ok(expected), "{}", case.name),
            (None, Some(fault)) => assert_eq!(outcome, Err(fault), "{}", case.name),
            _ => panic!("{} names neither a value nor a fault", case.name),
        }
    }
}

fn hex_bytes(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&text[index..index + 2], 16).expect("hex byte"))
        .collect()
}
