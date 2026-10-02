#![cfg(feature = "cbor")]

use laser_wire::clients::{ProducerLatency, ProducerPresence, ProducerStatistics};

#[test]
fn given_producer_presence_when_encoded_then_should_match_cross_sdk_fixture() {
    let presence = ProducerPresence {
        producer_presence_version: 1,
        observed_at_millis: 1000,
        expires_after_millis: 30_000,
        producers: vec![ProducerStatistics {
            instance_id: "producer-1".into(),
            stream: "stream".into(),
            topic: "topic".into(),
            first_activity_millis: 1,
            last_activity_millis: 900,
            submitted_records: 5,
            submitted_payload_bytes: 20,
            confirmed_records: Some(3),
            confirmed_payload_bytes: Some(12),
            retries: None,
            failed_calls: 1,
            last_success_millis: Some(800),
            latency: ProducerLatency {
                samples: 2,
                p50_micros: Some(32),
                p99_micros: None,
                p999_micros: None,
            },
        }],
    };
    let fixture = include_bytes!("../fixtures/producer_presence.bin");
    assert_eq!(
        laser_wire::framing::encode_named(&presence).unwrap(),
        fixture
    );
    assert_eq!(
        laser_wire::framing::decode_named::<ProducerPresence>(fixture).unwrap(),
        presence
    );
}
