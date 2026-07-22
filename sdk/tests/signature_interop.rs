#![cfg(feature = "sign")]

use laser_sdk::sign::{KeyRegistry, SigningKey};
use laser_wire::agent::{
    AgentEnvelope, ConversationId, CorrelationId, RecordId, SIGNATURE_SCHEME_ED25519, Signature,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct SignatureVector {
    secret_seed_hex: String,
    public_key_hex: String,
    key_id_hex: String,
    rust_signature_hex: String,
    typescript_signature_hex: String,
}

fn vector() -> SignatureVector {
    serde_json::from_str(include_str!("fixtures/typescript_signature.json"))
        .expect("signature vector parses")
}

fn envelope() -> AgentEnvelope {
    AgentEnvelope::command(
        RecordId::from_u128(1),
        ConversationId::from_u128(2),
        "planner".parse().expect("agent id parses"),
        CorrelationId::from_u128(3),
        b"{}".to_vec(),
    )
}

fn decode_hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).expect("hex is utf8");
            u8::from_str_radix(text, 16).expect("hex byte parses")
        })
        .collect()
}

#[test]
fn given_the_shared_vector_when_rust_signs_then_should_match_the_typescript_signature() {
    let vector = vector();
    let secret: [u8; 32] = decode_hex(&vector.secret_seed_hex)
        .try_into()
        .expect("secret seed is 32 bytes");
    let key = SigningKey::from_bytes(&secret);
    let signature = key.sign(&envelope()).expect("envelope signs");

    assert_eq!(
        key.verifying_key().as_bytes().as_slice(),
        decode_hex(&vector.public_key_hex).as_slice()
    );
    assert_eq!(key.key_id(), decode_hex(&vector.key_id_hex));
    assert_eq!(signature.bytes, decode_hex(&vector.rust_signature_hex));
}

#[test]
fn given_a_typescript_signature_when_rust_verifies_then_should_authenticate_the_principal() {
    let vector = vector();
    let secret: [u8; 32] = decode_hex(&vector.secret_seed_hex)
        .try_into()
        .expect("secret seed is 32 bytes");
    let key = SigningKey::from_bytes(&secret);
    let mut registry = KeyRegistry::new();
    registry.enroll("typescript-agent", key.verifying_key());
    let signed = envelope().with_signature(Signature {
        scheme: SIGNATURE_SCHEME_ED25519,
        key_id: decode_hex(&vector.key_id_hex),
        bytes: decode_hex(&vector.typescript_signature_hex),
        context: None,
    });

    assert_eq!(
        registry
            .verify(&signed)
            .expect("TypeScript signature verifies"),
        "typescript-agent"
    );
}
