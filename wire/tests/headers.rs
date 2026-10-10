#![cfg(feature = "cbor")]

// The shared header blocks of the envelope and generic record families.
// TypeScript asserts the same files, and the Rust SDK and Python stamp records
// through this encoder.

use laser_wire::agent::{AgentEnvelope, ConversationId, CorrelationId, RecordId};
use laser_wire::content::ContentType;
use laser_wire::headers::{Addressee, HeaderField, RecordHeaders};
use serde_json::{Value as Json, json};
use std::path::PathBuf;

const REGEN_ENV: &str = "AGDX_WIRE_FIXTURES_REGEN";

#[test]
fn given_each_record_family_when_headers_are_encoded_then_should_match_the_shared_block() {
    let mut envelope = AgentEnvelope::command(
        RecordId::from_u128(1),
        ConversationId::from_u128(2),
        "planner".parse().expect("agent id"),
        CorrelationId::from_u128(3),
        b"{}".to_vec(),
    );
    envelope.parent = Some(ConversationId::from_u128(4));
    envelope.root = Some(ConversationId::from_u128(5));
    assert_block(
        "header_block_envelope.json",
        &RecordHeaders::for_envelope(&envelope, ContentType::Json, true),
    );
    let mut generic = RecordHeaders::new(ConversationId::from_u128(2));
    generic.parent = Some(ConversationId::from_u128(4));
    generic.root = Some(ConversationId::from_u128(5));
    generic.agent = Some("planner".parse().expect("agent id"));
    generic.addressee = Some(Addressee::Agent("critic".parse().expect("agent id")));
    generic.causal_parent = Some("1:42".to_owned());
    generic.idempotency_key = Some("job-1".to_owned());
    generic.correlation = Some("corr-1".to_owned());
    generic.fence = Some(7);
    generic.deadline_micros = Some(1_717_171_717_000_000);
    generic.input_tokens = Some(100);
    generic.output_tokens = Some(50);
    generic.cost_usd = Some(0.25);
    assert_block("header_block_generic.json", &generic);
}

#[test]
fn given_broadcast_when_parsed_then_should_never_become_an_agent_id() {
    assert_eq!(
        Addressee::parse("*").expect("broadcast"),
        Addressee::Broadcast
    );
    assert_eq!(Addressee::Broadcast.as_str(), "*");
    assert!(matches!(
        Addressee::parse("planner").expect("agent"),
        Addressee::Agent(_)
    ));
}

fn assert_block(name: &str, headers: &RecordHeaders) {
    let block: Vec<Json> = headers
        .encode()
        .into_iter()
        .map(|(key, field)| match field {
            HeaderField::Text(value) => json!({ "key": key, "kind": "text", "value": value }),
            HeaderField::Uint8(value) => json!({ "key": key, "kind": "uint8", "value": value }),
            HeaderField::Uint32(value) => json!({ "key": key, "kind": "uint32", "value": value }),
            HeaderField::Uint64(value) => {
                json!({ "key": key, "kind": "uint64", "value": value.to_string() })
            }
            HeaderField::Float64(value) => {
                json!({ "key": key, "kind": "float64", "value": value })
            }
        })
        .collect();
    let encoded = serde_json::to_string_pretty(&block).expect("block encodes") + "\n";
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name);
    if std::env::var(REGEN_ENV).is_ok() {
        std::fs::write(&path, &encoded).expect("write header block");
    }
    let golden = std::fs::read_to_string(&path).expect("read header block");
    assert_eq!(encoded, golden, "header block `{name}` drifted");
}
