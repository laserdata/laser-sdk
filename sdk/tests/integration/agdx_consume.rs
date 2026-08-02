use crate::harness;
use async_trait::async_trait;
use laser_sdk::iggy::prelude::{HeaderKey, HeaderValue};
use laser_sdk::prelude::full::*;
use laser_sdk::wire::agent::{
    AgentDeadLetter, AgentEnvelope, AgentKind, ConversationId, CorrelationId, OPERATION_CHAT,
};
use laser_sdk::wire::codes::AGENT_OP_VERSION;
use laser_sdk::wire::content::ContentType;
use laser_sdk::wire::framing::decode_named;
use laser_sdk::wire::headers::{AGENT_VERSION, CONTENT_TYPE};
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Capture {
    seen: Arc<Mutex<Vec<AgentEnvelope>>>,
}

impl AgentHandler for Capture {
    async fn handle(&self, message: &AgentMessage, _ctx: &AgentCtx<'_>) -> Result<(), LaserError> {
        if let Some(envelope) = &message.envelope {
            self.seen
                .lock()
                .expect("the lock should not be poisoned")
                .push(envelope.clone());
        }
        Ok(())
    }
}

struct RecordingSink {
    capsules: Arc<Mutex<Vec<AgentDeadLetter>>>,
    failures: Arc<AtomicUsize>,
}

#[async_trait]
impl DeadLetterSink for RecordingSink {
    async fn on_dead_letter(
        &self,
        _message: Option<&AgentMessage>,
        capsule: &AgentDeadLetter,
        publish_result: &Result<(), LaserError>,
    ) {
        if publish_result.is_err() {
            self.failures.fetch_add(1, Ordering::SeqCst);
        }
        self.capsules
            .lock()
            .expect("the lock should not be poisoned")
            .push(capsule.clone());
    }
}

struct CountingMiddleware {
    before: Arc<AtomicUsize>,
}

#[async_trait]
impl AgentMiddleware for CountingMiddleware {
    async fn before_handle(&self, _message: &AgentMessage) -> Result<(), LaserError> {
        self.before.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn fixture(name: &str) -> &'static [u8] {
    laser_sdk::wire::fixtures::ALL
        .iter()
        .find_map(|(candidate, bytes)| (*candidate == name).then_some(*bytes))
        .expect("the fixture is registered")
}

async fn publish_fixture(laser: &Laser, bytes: &[u8]) {
    let mut headers = BTreeMap::new();
    headers.insert(
        HeaderKey::from_str(AGENT_VERSION).expect("the header key is valid"),
        HeaderValue::from(AGENT_OP_VERSION),
    );
    headers.insert(
        HeaderKey::from_str(CONTENT_TYPE).expect("the header key is valid"),
        HeaderValue::from(ContentType::Cbor.code()),
    );
    laser
        .topic(AgentTopic::Commands.topic_string())
        .send(bytes.to_vec(), headers, None)
        .await
        .expect("the fixture should publish");
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_an_agdx_command_when_consumed_then_the_handler_should_see_the_decoded_envelope() {
    let laser = harness::laser().await;
    let seen = Arc::new(Mutex::new(Vec::new()));

    Agent::builder()
        .id("worker".parse().expect("worker is a valid agent id"))
        .listen_on(AgentTopic::Commands)
        .handler(Capture { seen: seen.clone() })
        .build()
        .spawn(laser.clone());

    // Publish a typed AGDX command (not a `send_agent` message), tunneling the
    // foreign payload byte-identical in the body with `agdx.ct = json`.
    let conversation = ConversationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_0009);
    let correlation = CorrelationId::from_u128(0x0190_3c1f_aa00_0000_0000_0000_0000_000a);
    let params = br#"{"ask":"plan the trip"}"#.to_vec();
    laser
        .agdx(
            AgentTopic::Commands,
            "client".parse().expect("client is a valid agent id"),
            conversation,
        )
        .command(correlation, params.clone())
        .with_operation(OPERATION_CHAT)
        .content_type(ContentType::Json)
        .send()
        .await
        .expect("the AGDX command should publish");

    let envelopes = harness::eventually(|| {
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

    assert_eq!(envelopes.len(), 1);
    let envelope = &envelopes[0];
    assert_eq!(envelope.kind, AgentKind::Command);
    assert_eq!(envelope.conversation, conversation);
    assert_eq!(envelope.correlation, Some(correlation));
    assert_eq!(envelope.source.as_str(), "client");
    // The tunneled remainder reaches the handler byte-identical.
    assert_eq!(envelope.body, params);
}

#[tokio::test]
#[serial_test::serial(integration)]
async fn given_invalid_and_unmet_agdx_records_when_consumed_then_should_reject_before_dispatch() {
    const INVALID_FIXTURES: &[&str] = &[
        "agent_invalid_chunk_late_deadline.bin",
        "agent_invalid_chunk_no_sequence.bin",
        "agent_invalid_chunk_open_no_operation.bin",
        "agent_invalid_command_no_correlation.bin",
        "agent_invalid_error_last.bin",
        "agent_invalid_event_task_state.bin",
        "agent_invalid_response_channel.bin",
        "agent_invalid_status_bad_operation.bin",
        "agent_invalid_status_no_operation.bin",
    ];

    let laser = harness::laser().await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let capsules = Arc::new(Mutex::new(Vec::new()));
    let failures = Arc::new(AtomicUsize::new(0));
    let middleware_calls = Arc::new(AtomicUsize::new(0));
    let mut rejecting = Agent::builder()
        .id("strict-worker".parse().expect("the agent id is valid"))
        .listen_on(AgentTopic::Commands)
        .handler(Capture { seen: seen.clone() })
        .middleware(vec![Arc::new(CountingMiddleware {
            before: middleware_calls.clone(),
        })])
        .on_dead_letter(Arc::new(RecordingSink {
            capsules: capsules.clone(),
            failures: failures.clone(),
        }))
        .build()
        .spawn(laser.clone());
    rejecting.ready().await.expect("the agent should be ready");

    for name in INVALID_FIXTURES {
        publish_fixture(&laser, fixture(name)).await;
    }
    let required: AgentEnvelope =
        decode_named(fixture("agent_must_understand.bin")).expect("the fixture should decode");
    assert_ne!(required.must_understand, 0);
    publish_fixture(&laser, fixture("agent_must_understand.bin")).await;

    harness::eventually(|| {
        let capsules = capsules.clone();
        async move {
            (capsules
                .lock()
                .expect("the lock should not be poisoned")
                .len()
                == INVALID_FIXTURES.len() + 1)
                .then_some(())
        }
    })
    .await;
    assert!(
        seen.lock()
            .expect("the lock should not be poisoned")
            .is_empty()
    );
    assert_eq!(middleware_calls.load(Ordering::SeqCst), 0);
    assert_eq!(failures.load(Ordering::SeqCst), 0);
    rejecting
        .shutdown()
        .await
        .expect("the rejecting agent should drain");

    let mut understanding = Agent::builder()
        .id("strict-worker".parse().expect("the agent id is valid"))
        .listen_on(AgentTopic::Commands)
        .handler(Capture { seen: seen.clone() })
        .understood_features(required.must_understand)
        .build()
        .spawn(laser.clone());
    understanding
        .ready()
        .await
        .expect("the understanding agent should be ready");
    publish_fixture(&laser, fixture("agent_must_understand.bin")).await;

    harness::eventually(|| {
        let seen = seen.clone();
        async move {
            (seen
                .lock()
                .expect("the lock should not be poisoned")
                .len()
                == 1)
                .then_some(())
        }
    })
    .await;
    understanding
        .shutdown()
        .await
        .expect("the understanding agent should drain");
}
