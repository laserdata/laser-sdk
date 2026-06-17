use crate::harness;
use bytes::Bytes;
use iggy::prelude::IggyMessage;
use laser_sdk::prelude::*;
use laser_sdk::wire::agent::{AgentDeadLetter, DeadLetterReason};
use laser_sdk::wire::framing::decode_named;
use std::sync::Arc;
use std::sync::Mutex;

struct Noop;

impl AgentHandler for Noop {
    async fn handle(&self, _message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        Ok(())
    }
}

struct Collector {
    seen: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl AgentHandler for Collector {
    async fn handle(&self, message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        self.seen
            .lock()
            .expect("the lock should not be poisoned")
            .push(message.payload.clone());
        Ok(())
    }
}

#[tokio::test]
async fn given_a_message_without_provenance_when_consumed_then_should_dead_letter_the_raw_payload()
{
    let laser = harness::laser().await;
    let seen = Arc::new(Mutex::new(Vec::new()));

    // A worker consumes commands (its handler never runs: the message is
    // undecodable and dead-lettered first) and a collector drains the DLQ.
    Agent::builder()
        .id("worker".parse().expect("worker is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Noop)
        .build()
        .spawn(laser.clone());
    Agent::builder()
        .id("dlq-collector"
            .parse()
            .expect("collector is a valid agent id"))
        .listen_on(AgentTopic::Dlq)
        .handler(Collector { seen: seen.clone() })
        .build()
        .spawn(laser.clone());

    // Publish a header-less message, bypassing send_agent so it carries no
    // provenance and cannot be decoded by the consumer.
    let producer = laser
        .client()
        .producer(
            laser.stream().expect("test laser has a default stream"),
            "agent.commands",
        )
        .expect("the producer builder should be created")
        .build();
    producer
        .init()
        .await
        .expect("the producer should initialize");
    let message = IggyMessage::builder()
        .payload(Bytes::from_static(b"no-provenance"))
        .build()
        .expect("the message should build");
    producer
        .send(vec![message])
        .await
        .expect("the raw message should be sent");

    let dead = harness::eventually(|| {
        let seen = seen.clone();
        async move {
            let items = seen
                .lock()
                .expect("the lock should not be poisoned")
                .clone();
            (!items.is_empty()).then_some(items)
        }
    })
    .await;

    assert_eq!(dead.len(), 1);
    let capsule = decode_named::<AgentDeadLetter>(&dead[0])
        .expect("the dead-letter payload is an AgentDeadLetter capsule");
    assert_eq!(capsule.reason, DeadLetterReason::DecodeFailed);
    assert_eq!(capsule.attempts, 0);
    // The original bytes ride verbatim so redrive republishes them unchanged.
    assert_eq!(capsule.payload.as_slice(), b"no-provenance");
}
